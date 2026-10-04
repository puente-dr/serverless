import copy
import unittest
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


if __name__ == '__main__': unittest.main()
