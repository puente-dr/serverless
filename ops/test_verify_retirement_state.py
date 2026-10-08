"""Exercise drift and protected-reader failures without accessing AWS."""
import unittest
from unittest.mock import patch

import verify_retirement_state as ops


class RetirementVerificationTests(unittest.TestCase):
    def setUp(self):
        self.function_drift = False
        self.rule_drift = False
        self.reader_disabled = False

    def snapshot(self, manifest, account):
        functions = {name: 0 for name in manifest['disabled_functions']}
        rules = {name: 'DISABLED' for name in manifest['disabled_rules']}
        preserved = {name: None for name in manifest['preserved_functions']}
        if self.function_drift:
            functions[next(iter(functions))] = None
        if self.rule_drift and rules:
            rules[next(iter(rules))] = 'ENABLED'
        if self.reader_disabled:
            preserved[ops.READER] = 0
        return dict(account=account, functions_before=functions, rules_before=rules,
                    preserved_before=preserved)

    def verify(self):
        with patch.object(ops, 'snapshot', side_effect=self.snapshot) as snapshot:
            result = ops.verify('expected-account')
            self.assertEqual(snapshot.call_count, 3)
            return result

    def test_disabled_controls_and_preserved_reader_pass(self):
        result = self.verify()
        self.assertEqual(result['violations'], [])
        self.assertEqual(len(result['functions']), 10)
        self.assertEqual(len(result['rules']), 5)

    def test_function_capacity_drift_is_reported(self):
        self.function_drift = True
        self.assertTrue(any('function execution' in x for x in self.verify()['violations']))

    def test_enabled_schedule_is_reported(self):
        self.rule_drift = True
        self.assertTrue(any('schedule' in x for x in self.verify()['violations']))

    def test_disabled_reader_cannot_be_certified(self):
        self.reader_disabled = True
        self.assertIn('Protected reader execution is disabled', self.verify()['violations'])


if __name__ == '__main__':
    unittest.main()
