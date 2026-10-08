import copy
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import retire_integrations as r


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {'region': 'test', 'disabled_rules': ['schedule'],
                         'disabled_functions': ['unreserved', 'reserved'],
                         'preserved_functions': ['reader']}
        self.functions = {'unreserved': None, 'reserved': 2, 'reader': None}
        self.rules = {'schedule': 'ENABLED'}
        self.fail_on = None
        self.mutations = []
        self.mock = patch.object(r, 'aws', side_effect=self.aws)
        self.mock.start()
        self.addCleanup(self.mock.stop)

    def aws(self, region, service, operation, *args):
        if service == 'sts': return {'Account': 'expected'}
        name = args[1]
        if operation == 'get-function-configuration': return {'State': 'Active'}
        if operation == 'get-function-concurrency':
            return {} if self.functions[name] is None else {'ReservedConcurrentExecutions': self.functions[name]}
        if operation == 'describe-rule': return {'State': self.rules[name]}
        self.mutations.append((operation, name))
        if self.fail_on == name:
            self.fail_on = None
            raise RuntimeError('injected failure')
        if operation == 'disable-rule': self.rules[name] = 'DISABLED'
        elif operation == 'enable-rule': self.rules[name] = 'ENABLED'
        elif operation == 'put-function-concurrency': self.functions[name] = int(args[3])
        elif operation == 'delete-function-concurrency': self.functions[name] = None
        else: raise AssertionError(operation)
        return {}

    def test_plan_is_read_only(self):
        r.snapshot(self.manifest, 'expected')
        self.assertEqual(self.mutations, [])

    def test_wrong_account_blocks_all_mutations(self):
        with self.assertRaises(RuntimeError): r.snapshot(self.manifest, 'wrong')
        self.assertEqual(self.mutations, [])

    def test_protected_function_cannot_be_disabled(self):
        self.manifest['disabled_functions'].append('reader')
        with self.assertRaises(ValueError): r.snapshot(self.manifest, 'expected')
        self.assertEqual(self.mutations, [])

    def test_apply_and_rollback_preserve_exact_capacity(self):
        plan = r.snapshot(self.manifest, 'expected')
        r.apply(plan)
        self.assertEqual(self.functions, {'unreserved': 0, 'reserved': 0, 'reader': None})
        self.assertEqual(self.rules['schedule'], 'DISABLED')
        self.assertEqual(r.rollback(plan), plan)

    def test_partial_failure_restores_changed_resources(self):
        plan = r.snapshot(self.manifest, 'expected')
        self.fail_on = 'reserved'
        with self.assertRaises(RuntimeError): r.apply(plan)
        self.assertEqual(r.snapshot(self.manifest, 'expected'), plan)

    def test_drift_blocks_apply(self):
        plan = r.snapshot(self.manifest, 'expected')
        self.functions['reserved'] = 5
        with self.assertRaises(RuntimeError): r.apply(plan)
        self.assertEqual(self.mutations, [])

    def test_rollback_preserves_later_independent_retirement(self):
        plan = r.snapshot(self.manifest, 'expected')
        r.apply(plan)
        self.functions['reader'] = 0  # A separate, subsequently approved change.
        restored = r.rollback(plan)
        self.assertEqual(restored['functions_before'], plan['functions_before'])
        self.assertEqual(restored['rules_before'], plan['rules_before'])
        self.assertEqual(self.functions['reader'], 0)
        self.assertNotIn(('delete-function-concurrency', 'reader'), self.mutations)

    def test_partial_capacity_recovery_failure_keeps_schedule_disabled(self):
        plan = r.snapshot(self.manifest, 'expected')
        original = r.restore_function
        def restore(region, name, before):
            if name == 'unreserved':
                raise RuntimeError('Capacity recovery unavailable')
            return original(region, name, before)
        self.fail_on = 'reserved'
        with patch.object(r, 'restore_function', side_effect=restore):
            with self.assertRaisesRegex(RuntimeError, 'Partial rollback failed'):
                r.apply(plan)
        self.assertEqual(self.functions['unreserved'], 0)
        self.assertEqual(self.rules['schedule'], 'DISABLED')
        self.assertNotIn(('enable-rule', 'schedule'), self.mutations)

    def test_unverified_capacity_blocks_explicit_rollback_schedule(self):
        plan = r.snapshot(self.manifest, 'expected')
        r.apply(plan)
        self.mutations.clear()
        with patch.object(r, 'restore_one', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'capacity restoration not verified'):
                r.rollback(plan)
        self.assertEqual(self.rules['schedule'], 'DISABLED')
        self.assertEqual(self.mutations, [])

    def test_successful_response_without_disabling_capacity_is_rejected(self):
        plan = r.snapshot(self.manifest, 'expected')
        def ignore_disable(region, service, operation, *args):
            if operation == 'put-function-concurrency' and args[3] == '0':
                return {}  # A successful response is not state verification.
            return self.aws(region, service, operation, *args)
        with patch.object(r, 'aws', side_effect=ignore_disable):
            with self.assertRaisesRegex(RuntimeError, 'Function verification failed'):
                r.apply(plan)
        self.assertEqual(r.snapshot(self.manifest, 'expected'), plan)

    def test_modified_plan_cannot_change_protected_or_extra_resources(self):
        baseline = r.snapshot(self.manifest, 'expected')
        for action in (r.apply, r.rollback):
            for field, name, value in (('functions_before', 'reader', None),
                                       ('rules_before', 'other-schedule', 'ENABLED'),
                                       ('preserved_before', 'other-reader', None)):
                plan = copy.deepcopy(baseline)
                plan[field][name] = value
                with self.assertRaisesRegex(ValueError, 'resource scope'):
                    action(plan)
                self.assertEqual(self.mutations, [])

    def test_private_result_created_restricted_and_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'result.json'
            r.save(path, {'verified': True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                r.save(path, {'verified': False})
            self.assertIn('true', path.read_text())


if __name__ == '__main__': unittest.main()
