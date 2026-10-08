"""Exercise multi-instance retirement and rollback failures without AWS calls."""
import copy
import unittest
from unittest.mock import patch

import pause_map as ops


class RetiredEnvironmentTests(unittest.TestCase):
    def test_missing_environment_blocks_old_rollback_before_mutation(self):
        with patch.object(ops, 'check_account'), patch.object(ops, 'call',
                return_value={'Environments': []}) as call, patch.object(ops, 'gh') as gh:
            with self.assertRaisesRegex(RuntimeError, 'private-image recovery'):
                ops.rollback({'account': 'test'})
            call.assert_called_once_with('elasticbeanstalk', 'describe-environments',
                                        '--environment-names', ops.ENV)
            gh.assert_not_called()


class MapPauseTests(unittest.TestCase):
    def setUp(self):
        self.plan = dict(account='test', environment_id='env', version='version', cname='example.invalid',
            asg='group', capacity=[1, 4, 2], load_balancer='alb', targets=['target'], managed='false',
            instances={'first': dict(state='running', volumes=['disk1']),
                       'second': dict(state='running', volumes=['disk2'])},
            suspended=[], workflow_id=1, workflow_state='active')
        self.live = copy.deepcopy(self.plan)
        self.mutations = []
        self.partial_stop = False
        self.unhealthy = False
        replacements = dict(state=lambda account: copy.deepcopy(self.live), call=self.call, gh=self.gh,
            workflow=lambda: dict(state=self.live['workflow_state']),
            healthy=lambda plan: not self.unhealthy and ops.all_state(plan, 'running'),
            probe=lambda plan: ops.all_state(plan, 'running'), wait=self.wait,
            instance_states=lambda plan: {k: v['state'] for k, v in self.live['instances'].items()})
        for name, replacement in replacements.items():
            patcher = patch.object(ops, name, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def wait(self, predicate, description, seconds=900):
        if not predicate():
            raise RuntimeError('Fake timeout: ' + description)

    def gh(self, path, method='GET'):
        self.mutations.append(path.rsplit('/', 1)[-1])
        if path.endswith('/enable'):
            self.assertTrue(ops.all_state(self.plan, 'running'))
            self.assertFalse(self.unhealthy)
            self.live['workflow_state'] = 'active'
        elif path.endswith('/disable'):
            self.live['workflow_state'] = 'disabled_manually'
        else:
            self.fail('Unexpected GitHub mutation')

    def call(self, service, action, *args):
        self.mutations.append(action)
        if action == 'suspend-processes':
            self.live['suspended'] = sorted(set(self.live['suspended']) | set(ops.PROCESSES))
        elif action == 'resume-processes':
            self.assertFalse(self.unhealthy)
            self.assertTrue(ops.all_state(self.plan, 'running'))
            names = set(args[args.index('--scaling-processes') + 1:])
            self.live['suspended'] = sorted(set(self.live['suspended']) - names)
        elif action in ('stop-instances', 'start-instances'):
            ids = args[args.index('--instance-ids') + 1:]
            if action == 'stop-instances':
                self.assertEqual(set(ids), set(self.plan['instances']))
                self.assertNotEqual(self.live['workflow_state'], 'active')
                self.assertTrue(set(ops.PROCESSES).issubset(self.live['suspended']))
            for name in ids:
                self.live['instances'][name]['state'] = 'stopped' if action == 'stop-instances' else 'running'
                if action == 'stop-instances' and self.partial_stop:
                    self.partial_stop = False
                    raise RuntimeError('Only first instance stopped')
        else:
            self.fail('Unexpected AWS mutation')
        return {}

    def test_all_instances_stop_and_recover(self):
        after = ops.apply(self.plan)
        self.assertTrue(all(x['state'] == 'stopped' for x in after['instances'].values()))
        self.assertEqual(ops.rollback(self.plan), self.plan)

    def test_partial_stop_failure_recovers_only_stopped_instance(self):
        self.partial_stop = True
        with self.assertRaisesRegex(RuntimeError, 'Only first'):
            ops.apply(self.plan)
        self.assertEqual(self.live, self.plan)

    def test_unhealthy_target_keeps_automation_frozen(self):
        ops.apply(self.plan)
        self.unhealthy = True
        with self.assertRaisesRegex(RuntimeError, 'targets healthy'):
            ops.rollback(self.plan)
        self.assertEqual(self.live['workflow_state'], 'disabled_manually')
        self.assertTrue(set(ops.PROCESSES).issubset(self.live['suspended']))

    def test_new_instance_blocks_apply(self):
        self.live['instances']['third'] = dict(state='running', volumes=['disk3'])
        with self.assertRaisesRegex(RuntimeError, 'plan drift'):
            ops.apply(self.plan)
        self.assertEqual(self.mutations, [])

    def test_preserves_preexisting_disabled_workflow_and_scaling(self):
        self.plan.update(workflow_state='disabled_inactivity', suspended=['AlarmNotification'])
        self.live = copy.deepcopy(self.plan)
        ops.apply(self.plan)
        self.assertEqual(ops.rollback(self.plan), self.plan)
        self.assertNotIn('enable', self.mutations)


if __name__ == '__main__':
    unittest.main()
