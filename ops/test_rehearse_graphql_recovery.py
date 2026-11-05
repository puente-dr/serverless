"""Prevent a successful, failed or interrupted rehearsal from leaving test capacity."""
import tempfile
import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import rehearse_graphql_recovery as ops


class RehearsalCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / 'private-result'
        self.change = {'StackId': 'owned-test-stack'}
        self.writes = []
        self.execution_failure = None
        self.cleanup_failure = False

    def call(self, service, action, *args):
        self.writes.append(action)
        if action == 'execute-change-set' and self.execution_failure:
            raise self.execution_failure
        if action == 'describe-stacks':
            return {'Stacks': [{'StackStatus': 'DELETE_FAILED' if self.cleanup_failure else 'DELETE_COMPLETE'}]}
        return {}

    def run_rehearsal(self, failure=None):
        with patch.object(ops, 'reviewed_change_set', return_value=self.change), \
                patch.object(ops, 'call', side_effect=self.call), \
                patch.object(ops, 'wait_stack'), \
                patch.object(ops, 'run_probe', side_effect=failure):
            ops.rehearse('account', 'change-set', self.directory)

    def test_success_always_deletes_before_reporting_complete(self):
        self.run_rehearsal()
        self.assertEqual(self.writes, ['execute-change-set', 'delete-stack', 'describe-stacks'])
        self.assertTrue((self.directory / 'cleanup.json').exists())
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)

    def test_probe_failure_or_interrupt_still_deletes(self):
        for error in (RuntimeError('probe failed'), SystemExit(143), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                self.directory = Path(self.temp.name) / type(error).__name__
                with self.assertRaises(type(error)):
                    self.run_rehearsal(error)
                self.assertTrue((self.directory / 'cleanup.json').exists())

    def test_ambiguous_execution_failure_still_cleans_its_owned_stack(self):
        self.execution_failure = RuntimeError('lost execution response')
        with self.assertRaisesRegex(RuntimeError, 'lost execution response'):
            self.run_rehearsal()
        self.assertIn('delete-stack', self.writes)

    def test_cleanup_failure_cannot_report_success(self):
        self.cleanup_failure = True
        with self.assertRaisesRegex(RuntimeError, 'cleanup was not verified'):
            self.run_rehearsal()
        self.assertFalse((self.directory / 'cleanup.json').exists())

    def test_rejected_change_set_causes_no_execution_or_deletion(self):
        with patch.object(ops, 'reviewed_change_set', side_effect=RuntimeError('foreign stack')), \
                patch.object(ops, 'call') as call:
            with self.assertRaisesRegex(RuntimeError, 'foreign stack'):
                ops.rehearse('account', 'foreign', self.directory)
        call.assert_not_called()
        self.assertFalse(self.directory.exists())


class ChangeSetBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.change = dict(StackId='arn:aws:cloudformation:us-east-1:account:stack/' + ops.STACK + '/id',
            StackName=ops.STACK, Status='CREATE_COMPLETE', ExecutionStatus='AVAILABLE',
            Changes=[{'ResourceChange': {'LogicalResourceId': name, 'ResourceType': kind, 'Action': 'Add'}}
                     for name, kind in [('RecoveryInstance', 'AWS::EC2::Instance'),
                                        ('RecoverySecurityGroup', 'AWS::EC2::SecurityGroup')]],
            Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in
                {'RecoveryAmi': 'image', 'VpcId': 'vpc', 'SubnetId': 'subnet',
                 'TesterCidr': '192.0.2.1/32'}.items()])
        self.image = dict(OwnerId='account', Public=False, State='available',
                          BlockDeviceMappings=[{'Ebs': {'DeleteOnTermination': True}}])
        self.template = ops.TEMPLATE.read_text()

    def call(self, service, action, *args):
        return copy.deepcopy({'describe-change-set': self.change,
            'describe-stacks': {'Stacks': [{'StackStatus': 'REVIEW_IN_PROGRESS'}]},
            'get-template': {'TemplateBody': self.template},
            'describe-images': {'Images': [self.image]},
            'describe-image-attribute': {'LaunchPermissions': []}}[action])

    def review(self):
        with patch.object(ops, 'check_account'), patch.object(ops, 'call', side_effect=self.call):
            return ops.reviewed_change_set('account', 'change-set')

    def test_exact_private_recovery_change_set_passes(self):
        self.assertEqual(self.review(), self.change)

    def test_foreign_environment_or_modified_boot_template_is_rejected(self):
        self.change['StackName'] = 'original-production-environment'
        with self.assertRaisesRegex(RuntimeError, 'isolated create'):
            self.review()
        self.change['StackName'] = ops.STACK
        self.template += '\n# different reviewed template\n'
        with self.assertRaisesRegex(RuntimeError, 'local recovery template'):
            self.review()

    def test_public_image_or_retained_test_disk_is_rejected(self):
        self.image['Public'] = True
        with self.assertRaisesRegex(RuntimeError, 'private available image'):
            self.review()
        self.image['Public'] = False
        self.image['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'] = False
        with self.assertRaisesRegex(RuntimeError, 'automatically deleted test disk'):
            self.review()


if __name__ == '__main__':
    unittest.main()
