"""Probe only GraphQL protocol execution on an independently restored instance.

Uses the same verified, private, isolated target boundary as the map rehearsal.
No application resolvers, database calls, mutations, records or credentials.
"""
import argparse
import datetime
import hashlib
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

from probe_map_recovery import OPENER, call, target
from retire_integrations import save

STACK = 'puente-graphql-isolated-recovery-test'
HOST = 'puente-apollo-graphql-env.eba-836pg37x.us-east-1.elasticbeanstalk.com'
MAX_RESPONSE = 64 * 1024


def request(ip, label, payload, expected, get=False):
    path = '/graphql'
    headers = {'Host': HOST, 'Content-Type': 'application/json',
               'Apollo-Require-Preflight': 'true'}
    if get:
        path += '?' + urllib.parse.urlencode(payload)
    req = urllib.request.Request('http://' + ip + path,
        None if get else json.dumps(payload).encode(), headers)
    with OPENER.open(req, timeout=15) as response:
        raw = response.read(MAX_RESPONSE + 1)
        if response.status != 200 or len(raw) > MAX_RESPONSE:
            raise RuntimeError('Unexpected GraphQL response status or size: ' + label)
        if json.loads(raw) != {'data': expected}:
            raise RuntimeError('Unexpected GraphQL result: ' + label)
        return dict(status=response.status, bytes=len(raw),
                    sha256=hashlib.sha256(raw).hexdigest())


def probes(ip):
    payload = {'query': 'query RecoveryProbe { __typename }'}
    deadline = time.monotonic() + 600
    while True:
        try:
            first = request(ip, 'post_typename', payload, {'__typename': 'Query'})
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError('GraphQL did not become responsive during recovery')
            time.sleep(10)
    conditional = {'query': 'query RecoveryVariables($include: Boolean!) { '
                    'first: __typename second: __typename @include(if: $include) }',
                   'variables': {'include': False}}
    return dict(post_typename=first,
        post_variables=request(ip, 'post_variables', conditional, {'first': 'Query'}),
        get_typename=request(ip, 'get_typename', payload, {'__typename': 'Query'}, get=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--result', type=Path, required=True)
    parser.add_argument('--timing', type=Path)
    args = parser.parse_args()
    if args.result.exists():
        parser.error('Use a new private result file')
    stack, outputs = target(args.expected_account, STACK)
    responses = probes(outputs['TestIp'])
    deadline = time.monotonic() + 300
    while True:
        checks = call('ec2', 'describe-instance-status', '--instance-ids', outputs['InstanceId'])['InstanceStatuses']
        if len(checks) == 1 and all(checks[0][k]['Status'] == 'ok'
                                   for k in ('InstanceStatus', 'SystemStatus')):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('EC2 system/instance checks did not pass')
        time.sleep(10)
    target(args.expected_account, STACK)
    started = json.loads(args.timing.read_text())['started_at'] if args.timing else stack['CreationTime']
    start = datetime.datetime.fromisoformat(started.replace('Z', '+00:00'))
    end = datetime.datetime.now(datetime.timezone.utc)
    save(args.result, dict(started_at=start.isoformat(), verified_at=end.isoformat(),
        elapsed_seconds=round((end - start).total_seconds(), 2), instance_id=outputs['InstanceId'],
        isolation_verified=True, ec2_status_checks_passed=True, responses=responses,
        scope='GraphQL protocol only; database resolvers, routing and TLS untested'))
    print('Passed three GraphQL protocol probes and isolation/EC2 checks; metadata saved privately.')


if __name__ == '__main__':
    main()
