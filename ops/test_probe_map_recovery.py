"""Reject recovery target drift before any application HTTP request."""
import copy
import unittest
from unittest.mock import patch

import probe_map_recovery as ops


class RecoveryIsolationTests(unittest.TestCase):
    def setUp(self):
        self.stack = dict(StackId='stack-id', StackStatus='CREATE_COMPLETE',
            Outputs=[dict(OutputKey=k, OutputValue=v) for k, v in
                     {'InstanceId': 'instance', 'TestIp': '192.0.2.1'}.items()],
            Parameters=[dict(ParameterKey=k, ParameterValue=v) for k, v in
                        {'RecoveryAmi': 'image', 'VpcId': 'vpc', 'SubnetId': 'subnet',
                         'TesterCidr': '198.51.100.1/32'}.items()])
        self.resources = [dict(LogicalResourceId=k, PhysicalResourceId=v) for k, v in
                          {'RecoveryInstance': 'instance', 'RecoverySecurityGroup': 'group'}.items()]
        self.instance = dict(PublicIpAddress='192.0.2.1', ImageId='image', VpcId='vpc',
            SubnetId='subnet', MetadataOptions={'HttpTokens': 'required'},
            SecurityGroups=[{'GroupId': 'group'}],
            Tags=[{'Key': 'Purpose', 'Value': 'RetiredServiceRecoveryTest'}])
        self.image = dict(OwnerId='account', Public=False, State='available')
        self.permissions = []
        self.group = dict(IpPermissions=[dict(IpProtocol='tcp', FromPort=80, ToPort=80,
            IpRanges=[{'CidrIp': '198.51.100.1/32'}])],
            IpPermissionsEgress=[dict(IpProtocol='tcp', FromPort=9, ToPort=9,
                IpRanges=[{'CidrIp': '127.0.0.1/32'}])])

    def call(self, service, operation, *args):
        values = {'describe-stacks': {'Stacks': [self.stack]},
                  'describe-stack-resources': {'StackResources': self.resources},
                  'describe-instances': {'Reservations': [{'Instances': [self.instance]}]},
                  'describe-images': {'Images': [self.image]},
                  'describe-image-attribute': {'LaunchPermissions': self.permissions},
                  'describe-security-groups': {'SecurityGroups': [self.group]}}
        return copy.deepcopy(values[operation])

    def target(self):
        with patch.object(ops, 'check_account'), patch.object(ops, 'call', side_effect=self.call):
            return ops.target('account')

    def test_verified_private_recovery_target_passes(self):
        self.assertEqual(self.target()[1]['InstanceId'], 'instance')

    def test_foreign_instance_or_security_group_is_rejected(self):
        for field, value in (('RecoveryInstance', 'foreign-instance'),
                             ('RecoverySecurityGroup', 'production-group')):
            with self.subTest(field=field):
                original = copy.deepcopy(self.resources)
                next(x for x in self.resources if x['LogicalResourceId'] == field)['PhysicalResourceId'] = value
                with self.assertRaisesRegex(RuntimeError, 'ownership|identity'):
                    self.target()
                self.resources = original

    def test_public_shared_or_wrong_image_is_rejected(self):
        for field, value in (('Public', True), ('OwnerId', 'other-account'), ('State', 'pending')):
            with self.subTest(field=field):
                original = self.image[field]
                self.image[field] = value
                with self.assertRaisesRegex(RuntimeError, 'image is not private'):
                    self.target()
                self.image[field] = original
        self.permissions = [{'UserId': 'other-account'}]
        with self.assertRaisesRegex(RuntimeError, 'image is not private'):
            self.target()

    def test_network_or_role_drift_is_rejected(self):
        original = copy.deepcopy(self.instance)
        for field, value in (('ImageId', 'different-image'), ('IamInstanceProfile', {'Arn': 'role'}),
                             ('SubnetId', 'other-subnet'), ('VpcId', 'other-vpc'),
                             ('MetadataOptions', {'HttpTokens': 'optional'})):
            with self.subTest(field=field):
                self.instance[field] = value
                with self.assertRaisesRegex(RuntimeError, 'identity or isolation'):
                    self.target()
                self.instance = copy.deepcopy(original)
        self.group['IpPermissionsEgress'][0].update(IpProtocol='-1')
        with self.assertRaisesRegex(RuntimeError, 'egress'):
            self.target()

    def test_http_redirect_cannot_escape_verified_target(self):
        with self.assertRaisesRegex(RuntimeError, 'must not follow redirects'):
            ops.NoRedirects().redirect_request(None, None, 302, 'Found', {}, 'https://outside.invalid')


if __name__ == '__main__':
    unittest.main()
