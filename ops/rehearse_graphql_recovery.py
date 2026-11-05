"""Execute a reviewed GraphQL test change set with mandatory teardown on exit.

The change set must create exactly the local two-resource recovery template.
No changes to the original Beanstalk environment or its deployment controls.
"""
import argparse
import datetime
import ipaddress
import signal
import subprocess
import sys
from pathlib import Path

from probe_graphql_recovery import STACK
from retire_integrations import aws, check_account, save

REGION = 'us-east-1'
TEMPLATE = Path(__file__).with_name('graphql-recovery-test.yaml')


def call(*args):
    return aws(REGION, *args)


def wait_stack(operation, stack_id):
    subprocess.run(['aws', 'cloudformation', 'wait', operation,
        '--region', REGION, '--stack-name', stack_id, '--no-cli-pager'],
        capture_output=True, text=True, check=True, timeout=900)


def run_probe(account, directory):
    subprocess.run([sys.executable, str(Path(__file__).with_name('probe_graphql_recovery.py')),
        '--expected-account', account, '--timing', str(directory / 'launch-timing.json'),
        '--result', str(directory / 'probes.json')], check=True, timeout=1000)


def reviewed_change_set(account, change_set):
    check_account(REGION, account)
    change = call('cloudformation', 'describe-change-set', '--change-set-name', change_set)
    changes = [x['ResourceChange'] for x in change.get('Changes', [])]
    expected = {'RecoveryInstance': 'AWS::EC2::Instance',
                'RecoverySecurityGroup': 'AWS::EC2::SecurityGroup'}
    arn = change['StackId'].split(':', 5)
    if (change['StackName'] != STACK or change['Status'] != 'CREATE_COMPLETE'
            or change['ExecutionStatus'] != 'AVAILABLE' or len(changes) != 2
            or {x['LogicalResourceId']: x['ResourceType'] for x in changes} != expected
            or any(x['Action'] != 'Add' for x in changes)
            or len(arn) != 6 or arn[:5] != ['arn', 'aws', 'cloudformation', REGION, account]
            or not arn[5].startswith('stack/' + STACK + '/')):
        raise RuntimeError('Change set is not the expected isolated create operation')
    stack = call('cloudformation', 'describe-stacks', '--stack-name', change['StackId'])['Stacks'][0]
    if stack['StackStatus'] != 'REVIEW_IN_PROGRESS':
        raise RuntimeError('Refusing an existing or executing stack')
    body = call('cloudformation', 'get-template', '--change-set-name', change_set,
                '--template-stage', 'Original')['TemplateBody']
    if not isinstance(body, str) or body.strip() != TEMPLATE.read_text().strip():
        raise RuntimeError('Reviewed change set does not match the local recovery template')
    params = {x['ParameterKey']: x['ParameterValue'] for x in change['Parameters']}
    if set(params) != {'RecoveryAmi', 'VpcId', 'SubnetId', 'TesterCidr'}:
        raise RuntimeError('Unexpected recovery parameters')
    network = ipaddress.ip_network(params['TesterCidr'])
    if network.version != 4 or network.prefixlen != 32:
        raise RuntimeError('Expected a single operator IPv4 address')
    image = call('ec2', 'describe-images', '--image-ids', params['RecoveryAmi'])['Images'][0]
    permissions = call('ec2', 'describe-image-attribute', '--image-id', params['RecoveryAmi'],
                       '--attribute', 'launchPermission')['LaunchPermissions']
    mappings = image['BlockDeviceMappings']
    if (image['OwnerId'] != account or image['Public'] or image['State'] != 'available'
            or permissions or len(mappings) != 1 or 'Ebs' not in mappings[0]
            or not mappings[0]['Ebs'].get('DeleteOnTermination')):
        raise RuntimeError('Expected a private available image with one automatically deleted test disk')
    return change


def rehearse(account, change_set, directory):
    change = reviewed_change_set(account, change_set)  # Reject foreign stacks before any write.
    directory.mkdir(mode=0o700)
    save(directory / 'reviewed-change-set.json', change)
    save(directory / 'launch-timing.json',
         {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat()})
    stack_id = change['StackId']
    try:
        # Cleanup also covers a lost execution response: the operation may have applied.
        call('cloudformation', 'execute-change-set', '--change-set-name', change_set)
        wait_stack('stack-create-complete', stack_id)
        run_probe(account, directory)
    finally:
        # A second interrupt must not abandon teardown after the first one.
        previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            call('cloudformation', 'delete-stack', '--stack-name', stack_id)
            wait_stack('stack-delete-complete', stack_id)
            final = call('cloudformation', 'describe-stacks', '--stack-name', stack_id)['Stacks'][0]
            if final['StackStatus'] != 'DELETE_COMPLETE':
                raise RuntimeError('Recovery test cleanup was not verified')
            save(directory / 'cleanup.json', {'stack_id': stack_id,
                'status': final['StackStatus'],
                'verified_at': datetime.datetime.now(datetime.timezone.utc).isoformat()})
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--change-set', required=True, help='Previously previewed CREATE change set ARN')
    parser.add_argument('--result-dir', type=Path, required=True, help='New private directory')
    args = parser.parse_args()
    if args.result_dir.exists():
        parser.error('Use a new private result directory')

    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        rehearse(args.expected_account, args.change_set, args.result_dir)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    print('Recovery passed and temporary stack deletion verified; evidence saved privately.')


if __name__ == '__main__':
    main()
