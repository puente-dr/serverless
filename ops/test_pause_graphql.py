"""Failure and recovery tests use an in-memory AWS boundary, never real resources."""
import copy
import unittest
from unittest.mock import patch

import pause_graphql as ops


class PauseTests(unittest.TestCase):
    def setUp(self):
        self.plan = dict(account='test', environment_id='env', version='version', cname='example.invalid',
                         instance='instance', asg='group', volumes=['disk'], allocation='eip',
                         instance_state='running', suspended=[], managed='true', transition=True)
        self.live = copy.deepcopy(self.plan)
        self.writes = []
        self.fail_suspend = False
        self.fail_probe = False
        replacements = dict(
            state=lambda account: copy.deepcopy(self.live),
            probe=lambda plan: self.live['instance_state'] == 'running' and not self.fail_probe,
            call=self.call, managed=self.managed, transition=self.transition,
            no_deployment=lambda: self.live['transition'],
            ready=lambda: True, wait=self.wait,
            instance_state=lambda plan: self.live['instance_state'])
        self.patchers = [patch.object(ops, name, value) for name, value in replacements.items()]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def wait(self, predicate, description, seconds=900):
        if not predicate():
            raise RuntimeError('Fake timeout: ' + description)

    def managed(self, value):
        self.writes.append(('managed', value))
        self.live['managed'] = value

    def transition(self, value):
        self.writes.append(('transition', value))
        self.live['transition'] = value

    def call(self, service, action, *args):
        self.writes.append((service, action))
        if action == 'suspend-processes':
            # Simulate an API applying its change but losing its response.
            self.live['suspended'] = sorted(set(self.live['suspended']) | set(ops.PROCESSES))
            if self.fail_suspend:
                raise RuntimeError('Lost response')
        elif action == 'resume-processes':
            self.assertEqual(self.live['instance_state'], 'running')
            self.assertFalse(self.fail_probe)
            names = set(args[args.index('--scaling-processes') + 1:])
            self.live['suspended'] = sorted(set(self.live['suspended']) - names)
        elif action == 'stop-instances':
            self.assertEqual(self.live['managed'], 'false')
            self.assertFalse(self.live['transition'])
            self.assertTrue(set(ops.PROCESSES).issubset(self.live['suspended']))
            self.live['instance_state'] = 'stopped'
        elif action == 'start-instances':
            self.live['instance_state'] = 'running'
        else:
            self.fail('Unexpected resource mutation: ' + action)
        return {}

    def test_stop_and_exact_recovery(self):
        after = ops.apply(self.plan)
        self.assertEqual(after['instance_state'], 'stopped')
        self.assertEqual(ops.rollback(self.plan), self.plan)
        self.assertLess(self.writes.index(('ec2', 'start-instances')),
                        self.writes.index(('autoscaling', 'resume-processes')))

    def test_preserves_preexisting_disabled_controls(self):
        self.plan.update(suspended=['AlarmNotification'], transition=False, managed='false')
        self.live = copy.deepcopy(self.plan)
        ops.apply(self.plan)
        self.assertEqual(ops.rollback(self.plan), self.plan)

    def test_plan_drift_prevents_mutation(self):
        self.live['version'] = 'new-release'
        with self.assertRaisesRegex(RuntimeError, 'Plan drift'):
            ops.apply(self.plan)
        self.assertEqual(self.writes, [])

    def test_lost_suspend_response_restores_actual_state(self):
        self.fail_suspend = True
        with self.assertRaisesRegex(RuntimeError, 'Lost response'):
            ops.apply(self.plan)
        self.assertEqual(self.live, self.plan)
        self.assertNotIn(('ec2', 'stop-instances'), self.writes)

    def test_failed_application_recovery_keeps_controls_frozen(self):
        ops.apply(self.plan)
        self.fail_probe = True
        with self.assertRaisesRegex(RuntimeError, 'recovery probe'):
            ops.rollback(self.plan)
        self.assertFalse(self.live['transition'])
        self.assertEqual(self.live['managed'], 'false')
        self.assertTrue(set(ops.PROCESSES).issubset(self.live['suspended']))

    def test_replaced_disk_blocks_recovery(self):
        ops.apply(self.plan)
        self.live['volumes'] = ['replacement-disk']
        before = list(self.writes)
        with self.assertRaisesRegex(RuntimeError, 'Resource identity'):
            ops.rollback(self.plan)
        self.assertEqual(self.writes, before)

    def test_unrelated_suspension_not_undone(self):
        ops.apply(self.plan)
        self.live['suspended'].append('FutureProcess')
        before = list(self.writes)
        with self.assertRaisesRegex(RuntimeError, 'Unrelated'):
            ops.rollback(self.plan)
        self.assertEqual(self.writes, before)


if __name__ == '__main__':
    unittest.main()
