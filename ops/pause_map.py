"""Pause the owner-retired map behind its retained load balancer.

AWS plans/results are private. Requires AWS CLI and gh access. Auto-assigned
instance public IPs can change on restart; the retained ALB hostname is the route.
"""
import argparse
import json
import subprocess
import urllib.request
from pathlib import Path

from retire_integrations import aws, check_account, save
from pause_graphql import PROCESSES, wait

REGION = 'us-east-1'
ENV = 'puente-map-env'
APP = 'puente-map'
REPO = 'puente-dr/map'
WORKFLOW = '.github/workflows/beanstalk.yml'


def call(*args):
    return aws(REGION, *args)


def gh(path, method='GET'):
    result = subprocess.run(['gh', 'api', '--method', method, 'repos/' + REPO + '/' + path],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout or '{}')


def workflow():
    workflows = gh('actions/workflows')['workflows']
    selected = [x for x in workflows if x['path'] == WORKFLOW]
    if len(selected) != 1:
        raise RuntimeError('Expected exactly the map deployment workflow')
    selected = selected[0]
    runs = gh('actions/workflows/' + str(selected['id']) + '/runs?per_page=100')['workflow_runs']
    if any(x['status'] != 'completed' for x in runs):
        raise RuntimeError('Map deployment is running or waiting')
    if selected['state'] not in ('active', 'disabled_manually', 'disabled_inactivity'):
        raise RuntimeError('Unsupported workflow state')
    return selected


def state(account):
    check_account(REGION, account)
    env = call('elasticbeanstalk', 'describe-environments', '--environment-names', ENV)['Environments'][0]
    if env['ApplicationName'] != APP or env['Status'] != 'Ready':
        raise RuntimeError('Map environment is not ready')
    managed = call('elasticbeanstalk', 'describe-configuration-settings', '--application-name', APP,
                  '--environment-name', ENV, '--query',
                  "ConfigurationSettings[0].OptionSettings[?Namespace=='aws:elasticbeanstalk:managedactions' && OptionName=='ManagedActionsEnabled'].Value | [0]")
    if managed != 'false':
        raise RuntimeError('Map managed updates must already be disabled; review separately')
    resources = call('elasticbeanstalk', 'describe-environment-resources', '--environment-name', ENV)['EnvironmentResources']
    if len(resources['AutoScalingGroups']) != 1 or len(resources['LoadBalancers']) != 1:
        raise RuntimeError('Unexpected map resource topology')
    group = call('autoscaling', 'describe-auto-scaling-groups', '--auto-scaling-group-names',
                 resources['AutoScalingGroups'][0]['Name'])['AutoScalingGroups'][0]
    ids = sorted(x['InstanceId'] for x in group['Instances'])
    if (not ids or len(ids) != group['DesiredCapacity'] or len(group['TargetGroupARNs']) != 1
            or ids != sorted(x['Id'] for x in resources['Instances'])
            or any(x['LifecycleState'] != 'InService' for x in group['Instances'])):
        raise RuntimeError('Map is changing capacity or has unexpected membership')
    raw = call('ec2', 'describe-instances', '--instance-ids', *ids)
    instances = {}
    for reservation in raw['Reservations']:
        for instance in reservation['Instances']:
            if instance['RootDeviceType'] != 'ebs' or any('Ebs' not in b for b in instance['BlockDeviceMappings']):
                raise RuntimeError('Cannot preserve instance-store data')
            instances[instance['InstanceId']] = dict(state=instance['State']['Name'],
                volumes=sorted(b['Ebs']['VolumeId'] for b in instance['BlockDeviceMappings']))
    wf = workflow()
    return dict(account=account, environment_id=env['EnvironmentId'], version=env['VersionLabel'],
                cname=env['CNAME'], asg=group['AutoScalingGroupName'], instances=instances,
                capacity=[group[k] for k in ('MinSize', 'MaxSize', 'DesiredCapacity')],
                load_balancer=resources['LoadBalancers'][0]['Name'], targets=group['TargetGroupARNs'],
                suspended=sorted(x['ProcessName'] for x in group['SuspendedProcesses']),
                workflow_id=wf['id'], workflow_state=wf['state'], managed=managed)


def same_resources(plan, current):
    for key in ('account', 'environment_id', 'version', 'cname', 'asg', 'capacity',
                'load_balancer', 'targets', 'workflow_id', 'managed'):
        if plan[key] != current[key]:
            raise RuntimeError('Map identity/configuration changed; review before proceeding')
    if ({k: v['volumes'] for k, v in plan['instances'].items()}
            != {k: v['volumes'] for k, v in current['instances'].items()}):
        raise RuntimeError('Map instance or disk changed; review before proceeding')


def instance_states(plan):
    raw = call('ec2', 'describe-instances', '--instance-ids', *sorted(plan['instances']))
    return {x['InstanceId']: x['State']['Name'] for r in raw['Reservations'] for x in r['Instances']}


def all_state(plan, expected):
    values = instance_states(plan)
    return set(values) == set(plan['instances']) and all(x == expected for x in values.values())


def healthy(plan):
    descriptions = call('elbv2', 'describe-target-health', '--target-group-arn', plan['targets'][0])['TargetHealthDescriptions']
    found = {x['Target']['Id']: x['TargetHealth']['State'] for x in descriptions}
    return set(found) == set(plan['instances']) and all(v == 'healthy' for v in found.values())


def probe(plan):
    try:
        for path in ('/', '/_dash-dependencies', '/_dash-layout'):
            with urllib.request.urlopen('http://' + plan['cname'] + path, timeout=15) as response:
                if response.status != 200:
                    return False
                if path != '/':
                    json.load(response)
        return True
    except (OSError, ValueError):
        return False


def rollback(plan):
    current = state(plan['account'])
    same_resources(plan, current)
    if (set(current['suspended']) - (set(plan['suspended']) | set(PROCESSES))
            or set(plan['suspended']) - set(current['suspended'])):
        raise RuntimeError('Unrelated scaling changes require review')
    statuses = instance_states(plan)
    if any(v not in ('running', 'pending', 'stopping', 'stopped') for v in statuses.values()):
        raise RuntimeError('Unexpected instance state')
    if any(v in ('stopped', 'stopping') for v in statuses.values()):
        if not set(PROCESSES).issubset(current['suspended']):
            raise RuntimeError('Replacement safeguards missing')
        wait(lambda: all(v != 'stopping' for v in instance_states(plan).values()), 'map stop completion')
        stopped = [k for k, v in instance_states(plan).items() if v == 'stopped']
        if stopped:
            call('ec2', 'start-instances', '--instance-ids', *sorted(stopped))
    wait(lambda: all_state(plan, 'running'), 'map instances running')
    wait(lambda: healthy(plan), 'all map load-balancer targets healthy')
    wait(lambda: probe(plan), 'map root/layout/dependency probes')
    added = sorted((set(current['suspended']) & set(PROCESSES)) - set(plan['suspended']))
    if added:
        call('autoscaling', 'resume-processes', '--auto-scaling-group-name', plan['asg'], '--scaling-processes', *added)
    if plan['workflow_state'] == 'active' and current['workflow_state'] != 'active':
        gh('actions/workflows/' + str(plan['workflow_id']) + '/enable', 'PUT')
    after = state(plan['account'])
    if after != plan:
        raise RuntimeError('Map recovery differs from baseline')
    return after


def apply(plan):
    if state(plan['account']) != plan or not all_state(plan, 'running'):
        raise RuntimeError('Map plan drift or non-running baseline; no changes made')
    if not healthy(plan) or not probe(plan):
        raise RuntimeError('Map baseline probes failed; no changes made')
    try:
        if plan['workflow_state'] == 'active':
            gh('actions/workflows/' + str(plan['workflow_id']) + '/disable', 'PUT')
        workflow()  # Catch a deployment racing the initial check.
        call('autoscaling', 'suspend-processes', '--auto-scaling-group-name', plan['asg'], '--scaling-processes', *PROCESSES)
        current = state(plan['account'])
        same_resources(plan, current)
        if not set(PROCESSES).issubset(current['suspended']) or current['workflow_state'] == 'active':
            raise RuntimeError('Map automation controls did not settle')
        call('ec2', 'stop-instances', '--instance-ids', *sorted(plan['instances']))
        wait(lambda: all_state(plan, 'stopped'), 'map instances stopped')
        after = state(plan['account'])
        same_resources(plan, after)
        if not all(x['state'] == 'stopped' for x in after['instances'].values()):
            raise RuntimeError('Map stop verification failed')
        return after
    except BaseException as original:
        try:
            rollback(plan)
        except BaseException as recovery_error:
            raise RuntimeError('Map recovery incomplete; inspect private plan and controls') from recovery_error
        raise original


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'apply', 'rollback'])
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--result', type=Path)
    args = parser.parse_args()
    if args.action == 'plan':
        before = state(args.expected_account)
        if not all_state(before, 'running') or not healthy(before) or not probe(before):
            parser.error('Need a healthy, responsive map baseline')
        save(args.plan, before)
    else:
        if not args.result or args.result.exists():
            parser.error('Provide a new private --result path')
        before = json.loads(args.plan.read_text())
        if before['account'] != args.expected_account:
            parser.error('Plan account mismatch')
        save(args.result, apply(before) if args.action == 'apply' else rollback(before))
    print(args.action + ': map verified; instances, disks and load balancer retained.')


if __name__ == '__main__':
    main()
