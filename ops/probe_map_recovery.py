"""Read-only application probes for the isolated map recovery stack.

Verifies target isolation first. Reports response metadata, never records/tokens.
Requires AWS CLI credentials; the test instance itself must have no IAM profile.
"""
import argparse
import datetime
import hashlib
import ipaddress
import json
import time
import urllib.request
from pathlib import Path

from retire_integrations import aws, check_account, save

REGION = 'us-east-1'
STACK = 'puente-map-isolated-recovery-test'
HOST = 'puente-map-env.eba-cenm4eic.us-east-1.elasticbeanstalk.com'
EXPECTED_CALLBACKS = {
    'location-options-dropdown.options', 'location-options-dropdown.value',
    'health-options-dropdown.options', 'health-options-dropdown.value',
    'dd-output-container.children', 'display-selected-values.figure',
}


class NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError('Recovery probes must not follow redirects outside the verified target')


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirects())


def call(*args):
    return aws(REGION, *args)


def target(account):
    check_account(REGION, account)
    stack = call('cloudformation', 'describe-stacks', '--stack-name', STACK)['Stacks'][0]
    if stack['StackStatus'] != 'CREATE_COMPLETE':
        raise RuntimeError('Isolated recovery stack is not ready')
    outputs = {x['OutputKey']: x['OutputValue'] for x in stack['Outputs']}
    parameters = {x['ParameterKey']: x['ParameterValue'] for x in stack['Parameters']}
    resources = call('cloudformation', 'describe-stack-resources', '--stack-name', stack['StackId'])['StackResources']
    identities = {x['LogicalResourceId']: x['PhysicalResourceId'] for x in resources}
    if (len(resources) != 2 or set(identities) != {'RecoveryInstance', 'RecoverySecurityGroup'}
            or identities['RecoveryInstance'] != outputs['InstanceId']):
        raise RuntimeError('Recovery stack ownership differs from expected resources')
    if not isinstance(ipaddress.ip_address(outputs['TestIp']), ipaddress.IPv4Address):
        raise RuntimeError('Expected an IPv4 recovery target')
    instance = call('ec2', 'describe-instances', '--instance-ids', outputs['InstanceId'])['Reservations'][0]['Instances'][0]
    tags = {x['Key']: x['Value'] for x in instance.get('Tags', [])}
    if (tags.get('Purpose') != 'RetiredServiceRecoveryTest' or instance.get('IamInstanceProfile')
            or instance['PublicIpAddress'] != outputs['TestIp']
            or instance['ImageId'] != parameters['RecoveryAmi']
            or instance['SubnetId'] != parameters['SubnetId']
            or instance['VpcId'] != parameters['VpcId']
            or instance['MetadataOptions']['HttpTokens'] != 'required'
            or [x['GroupId'] for x in instance['SecurityGroups']] != [identities['RecoverySecurityGroup']]):
        raise RuntimeError('Recovery instance identity or isolation differs from expected state')
    image = call('ec2', 'describe-images', '--image-ids', instance['ImageId'])['Images'][0]
    permissions = call('ec2', 'describe-image-attribute', '--image-id', instance['ImageId'], '--attribute', 'launchPermission')
    if (image['OwnerId'] != account or image['Public'] or image['State'] != 'available'
            or permissions['LaunchPermissions']):
        raise RuntimeError('Recovery image is not private, unshared and available in the expected account')
    group = call('ec2', 'describe-security-groups', '--group-ids', instance['SecurityGroups'][0]['GroupId'])['SecurityGroups'][0]
    cidr = parameters['TesterCidr']
    if ipaddress.IPv4Network(cidr).prefixlen != 32:
        raise RuntimeError('Recovery ingress must use one operator IPv4 address')
    ingress, egress = group['IpPermissions'], group['IpPermissionsEgress']
    if (len(ingress) != 1 or ingress[0]['IpProtocol'] != 'tcp'
            or ingress[0]['FromPort'] != 80 or ingress[0]['ToPort'] != 80
            or ingress[0]['IpRanges'] != [{'CidrIp': cidr}] or not cidr.endswith('/32')
            or any(ingress[0].get(k) for k in ('Ipv6Ranges', 'PrefixListIds', 'UserIdGroupPairs'))):
        raise RuntimeError('Unexpected recovery ingress rules')
    if (len(egress) != 1 or egress[0]['IpProtocol'] != 'tcp'
            or egress[0]['FromPort'] != 9 or egress[0]['ToPort'] != 9
            or egress[0]['IpRanges'] != [{'CidrIp': '127.0.0.1/32'}]
            or any(egress[0].get(k) for k in ('Ipv6Ranges', 'PrefixListIds', 'UserIdGroupPairs'))):
        raise RuntimeError('Unexpected recovery egress rules')
    return stack, outputs


def probes(ip):
    summaries = {}

    def request(label, path, payload=None):
        headers = {'Host': HOST}
        if payload is not None:
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request('http://' + ip + path,
            json.dumps(payload).encode() if payload is not None else None, headers)
        with OPENER.open(req, timeout=20) as response:
            raw = response.read(10 * 1024 * 1024 + 1)
            if response.status != 200 or len(raw) > 10 * 1024 * 1024:
                raise RuntimeError('Unexpected recovery response status or size')
            summaries[label] = dict(status=response.status, bytes=len(raw),
                                    sha256=hashlib.sha256(raw).hexdigest())
            return json.loads(raw) if path != '/' else None

    deadline = time.monotonic() + 600
    while True:
        try:
            request('root', '/')
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError('Map did not become responsive during recovery')
            time.sleep(10)
    layout = request('layout', '/_dash-layout')
    dependencies = request('dependencies', '/_dash-dependencies')
    if {x['output'] for x in dependencies} != EXPECTED_CALLBACKS or len(dependencies) != 6:
        raise RuntimeError('Unexpected callback graph; review before invoking callbacks')
    props = {}

    def walk(value):
        if isinstance(value, dict):
            if isinstance(value.get('props'), dict) and isinstance(value['props'].get('id'), str):
                props[value['props']['id']] = value['props']
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(layout)
    for dependency in dependencies:
        output = dependency['output']
        component, prop = output.rsplit('.', 1)
        payload = dict(output=output, outputs=dict(id=component, property=prop), state=[],
            inputs=[dict(x, value=props[x['id']].get(x['property'])) for x in dependency['inputs']],
            changedPropIds=[x['id'] + '.' + x['property'] for x in dependency['inputs']])
        data = request(output, '/_dash-update-component', payload)
        props.setdefault(component, {})[prop] = data['response'][component][prop]
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--result', required=True, type=Path)
    parser.add_argument('--timing', type=Path, help='Private JSON containing the launch started_at time')
    args = parser.parse_args()
    if args.result.exists():
        parser.error('Use a new private result file')
    stack, outputs = target(args.expected_account)
    responses = probes(outputs['TestIp'])
    target(args.expected_account)  # Confirm isolation did not change during probes.
    checks = call('ec2', 'describe-instance-status', '--instance-ids', outputs['InstanceId'])['InstanceStatuses']
    if len(checks) != 1 or any(checks[0][k]['Status'] != 'ok' for k in ('InstanceStatus', 'SystemStatus')):
        raise RuntimeError('EC2 system/instance checks have not both passed; retry verification')
    started = json.loads(args.timing.read_text())['started_at'] if args.timing else stack['CreationTime']
    start = datetime.datetime.fromisoformat(started.replace('Z', '+00:00'))
    end = datetime.datetime.now(datetime.timezone.utc)
    save(args.result, dict(started_at=start.isoformat(), verified_at=end.isoformat(),
        elapsed_seconds=round((end - start).total_seconds(), 2), instance_id=outputs['InstanceId'],
        isolation_verified=True, ec2_status_checks_passed=True, responses=responses))
    print('Passed nine read-only map probes and isolation/EC2 checks; metadata saved privately.')


if __name__ == '__main__':
    main()
