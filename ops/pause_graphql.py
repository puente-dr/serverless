"""Reversible pause for the owner-retired, single-instance GraphQL environment.

This is a bounded operational retirement, not ownership of Beanstalk's child stack.
Keep its plan outside the public repository. Retains EBS and EIP charges.
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

from retire_integrations import aws, check_account, save

REGION = 'us-east-1'
ENV = 'puente-apollo-graphql-env'
APP = 'puente-apollo-graphql'
PIPELINE = 'puente-graphql-api'
STAGE = 'Deploy'
PROCESSES = ['Launch', 'Terminate', 'HealthCheck', 'ReplaceUnhealthy',
             'AZRebalance', 'AlarmNotification', 'ScheduledActions',
             'AddToLoadBalancer', 'InstanceRefresh']
NS = 'aws:elasticbeanstalk:managedactions'


def call(*args):
    return aws(REGION, *args)


def environment():
    return call('elasticbeanstalk', 'describe-environments',
                '--environment-names', ENV)['Environments'][0]


def ready():
    return environment()['Status'] == 'Ready'


def wait(predicate, description, seconds=900):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(10)
    raise RuntimeError('Timed out: ' + description)


def no_deployment():
    state = call('codepipeline', 'get-pipeline-state', '--name', PIPELINE)
    for stage in state['stageStates']:
        executions = [stage.get('latestExecution', {}), stage.get('inboundExecution', {})]
        executions += stage.get('inboundExecutions', [])
        if any(x.get('status') in ('InProgress', 'Stopping') for x in executions):
            raise RuntimeError('Pipeline execution is active or waiting; resolve it before retirement')
        if any(a.get('latestExecution', {}).get('status') == 'InProgress'
               for a in stage.get('actionStates', [])):
            raise RuntimeError('Pipeline action is running')
    actions = call('elasticbeanstalk', 'describe-environment-managed-actions',
                   '--environment-name', ENV)['ManagedActions']
    if any(x['Status'] == 'Running' for x in actions):
        raise RuntimeError('Managed update is running')
    pipeline = call('codepipeline', 'get-pipeline', '--name', PIPELINE)['pipeline']
    deploy = next(s for s in pipeline['stages'] if s['name'] == STAGE)
    if len(deploy['actions']) != 1:
        raise RuntimeError('Unexpected deployment actions')
    action = deploy['actions'][0]
    if (action['actionTypeId']['provider'] != 'ElasticBeanstalk'
            or action['configuration'].get('EnvironmentName') != ENV
            or action['configuration'].get('ApplicationName') != APP):
        raise RuntimeError('Pipeline is not exclusively the expected GraphQL deployment')
    return next(s for s in state['stageStates'] if s['stageName'] == STAGE)['inboundTransitionState']['enabled']


def state(account):
    check_account(REGION, account)
    env = environment()
    if env['ApplicationName'] != APP or env['Status'] != 'Ready':
        raise RuntimeError('Environment is not ready or belongs to a different application')
    resources = call('elasticbeanstalk', 'describe-environment-resources',
                     '--environment-name', ENV)['EnvironmentResources']
    if len(resources['Instances']) != 1 or len(resources['AutoScalingGroups']) != 1 or resources['LoadBalancers']:
        raise RuntimeError('Expected one instance, one Auto Scaling group and no load balancer')
    iid = resources['Instances'][0]['Id']
    asg_name = resources['AutoScalingGroups'][0]['Name']
    asg = call('autoscaling', 'describe-auto-scaling-groups',
               '--auto-scaling-group-names', asg_name)['AutoScalingGroups'][0]
    if ([x['InstanceId'] for x in asg['Instances']] != [iid]
            or asg['Instances'][0]['LifecycleState'] != 'InService'
            or [asg[k] for k in ('MinSize', 'MaxSize', 'DesiredCapacity')] != [1, 1, 1]):
        raise RuntimeError('Unexpected group membership or capacity')
    instance = call('ec2', 'describe-instances', '--instance-ids', iid)['Reservations'][0]['Instances'][0]
    if instance['RootDeviceType'] != 'ebs' or any('Ebs' not in b for b in instance['BlockDeviceMappings']):
        raise RuntimeError('Cannot preserve instance-store data by stopping')
    addresses = call('ec2', 'describe-addresses', '--filters', 'Name=instance-id,Values=' + iid)['Addresses']
    if len(addresses) != 1:
        raise RuntimeError('Expected retained Elastic IP')
    managed = call('elasticbeanstalk', 'describe-configuration-settings',
                   '--application-name', APP, '--environment-name', ENV, '--query',
                   "ConfigurationSettings[0].OptionSettings[?Namespace=='" + NS +
                   "' && OptionName=='ManagedActionsEnabled'].Value | [0]")
    if managed not in ('true', 'false'):
        raise RuntimeError('Unexpected managed-update configuration')
    transition = no_deployment()
    return dict(account=account, environment_id=env['EnvironmentId'], version=env['VersionLabel'],
                cname=env['CNAME'], instance=iid, asg=asg_name,
                volumes=sorted(b['Ebs']['VolumeId'] for b in instance['BlockDeviceMappings']),
                allocation=addresses[0]['AllocationId'], instance_state=instance['State']['Name'],
                suspended=sorted(x['ProcessName'] for x in asg['SuspendedProcesses']),
                managed=managed, transition=transition)


def probe(plan):
    request = urllib.request.Request('http://' + plan['cname'] + '/graphql',
        json.dumps({'query': 'query RetirementProbe { __typename }'}).encode(),
        {'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.load(response)
            return response.status == 200 and body.get('data', {}).get('__typename') == 'Query' and not body.get('errors')
    except (OSError, ValueError):
        return False


def managed(value):
    call('elasticbeanstalk', 'update-environment', '--environment-name', ENV,
         '--option-settings', json.dumps([dict(Namespace=NS, OptionName='ManagedActionsEnabled', Value=value)]))
    wait(ready, 'Beanstalk configuration update')


def transition(enabled):
    args = ['codepipeline', 'enable-stage-transition' if enabled else 'disable-stage-transition',
            '--pipeline-name', PIPELINE, '--stage-name', STAGE, '--transition-type', 'Inbound']
    if not enabled:
        args += ['--reason', 'Owner-retired GraphQL; deployment held for reversible compute pause']
    call(*args)


def instance_state(plan):
    return call('ec2', 'describe-instances', '--instance-ids', plan['instance'])['Reservations'][0]['Instances'][0]['State']['Name']


def same_resources(plan, current):
    keys = ['account', 'environment_id', 'version', 'cname', 'instance', 'asg', 'volumes', 'allocation']
    if any(plan[k] != current[k] for k in keys):
        raise RuntimeError('Resource identity or application version changed; stop for review')


def rollback(plan):
    current = state(plan['account'])
    same_resources(plan, current)
    expected_suspensions = set(plan['suspended']) | set(PROCESSES)
    if set(current['suspended']) - expected_suspensions or set(plan['suspended']) - set(current['suspended']):
        raise RuntimeError('Unrelated scaling-process changes need review before recovery')
    if current['instance_state'] in ('stopping', 'stopped'):
        if not set(PROCESSES).issubset(current['suspended']):
            raise RuntimeError('Stopped instance lacks replacement safeguards; review before recovery')
        wait(lambda: instance_state(plan) == 'stopped', 'instance stopped')
        call('ec2', 'start-instances', '--instance-ids', plan['instance'])
    elif current['instance_state'] not in ('running', 'pending'):
        raise RuntimeError('Unexpected instance state during recovery')
    wait(lambda: instance_state(plan) == 'running', 'instance running')
    # Never resume replacement processes or deployment until the retained app responds.
    wait(lambda: probe(plan), 'GraphQL read-only recovery probe')
    added = sorted((set(current['suspended']) & set(PROCESSES)) - set(plan['suspended']))
    if added:
        call('autoscaling', 'resume-processes', '--auto-scaling-group-name', plan['asg'],
             '--scaling-processes', *added)
    if current['managed'] != plan['managed']:
        managed(plan['managed'])
    if current['transition'] != plan['transition']:
        transition(plan['transition'])
    after = state(plan['account'])
    if after != plan or not probe(plan):
        raise RuntimeError('Recovery verification differs from baseline')
    return after


def apply(plan):
    if state(plan['account']) != plan or plan['instance_state'] != 'running':
        raise RuntimeError('Plan drift or non-running baseline; no changes made')
    if not probe(plan):
        raise RuntimeError('Baseline GraphQL probe failed; no changes made')
    try:
        transition(False)
        no_deployment()  # Detect an execution that raced the initial check.
        if plan['managed'] != 'false':
            managed('false')
        current = state(plan['account'])
        same_resources(plan, current)
        call('autoscaling', 'suspend-processes', '--auto-scaling-group-name', plan['asg'],
             '--scaling-processes', *PROCESSES)
        current = state(plan['account'])
        if (not set(PROCESSES).issubset(current['suspended']) or current['managed'] != 'false'
                or current['transition']):
            raise RuntimeError('Retirement controls did not settle')
        call('ec2', 'stop-instances', '--instance-ids', plan['instance'])
        wait(lambda: instance_state(plan) == 'stopped', 'instance stopped')
        after = state(plan['account'])
        same_resources(plan, after)
        if after['instance_state'] != 'stopped':
            raise RuntimeError('Stop not verified')
        return after
    except BaseException as error:
        try:
            # A failed EB update may still be settling; do not act on stale state.
            wait(ready, 'Beanstalk ready for recovery')
            rollback(plan)
        except BaseException as recovery_error:
            raise RuntimeError('Automatic recovery incomplete; use private plan and inspect controls') from recovery_error
        raise error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'apply', 'rollback'])
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--result', type=Path)
    args = parser.parse_args()
    if args.action == 'plan':
        before = state(args.expected_account)
        if before['instance_state'] != 'running' or not probe(before):
            parser.error('Need a running, responsive GraphQL baseline')
        save(args.plan, before)
    else:
        if not args.result or args.result.exists():
            parser.error('Provide a new private --result path')
        plan = json.loads(args.plan.read_text())
        if plan['account'] != args.expected_account:
            parser.error('Plan account mismatch')
        result = apply(plan) if args.action == 'apply' else rollback(plan)
        save(args.result, result)
    print(args.action + ': verified; retained resource definitions, EBS and Elastic IP.')


if __name__ == '__main__':
    main()
