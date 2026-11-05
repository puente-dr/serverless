"""Protocol probes must remain record-free and fail on unexpected responses."""
import json
import unittest
from unittest.mock import MagicMock, patch

import probe_graphql_recovery as ops


class GraphQLRecoveryTests(unittest.TestCase):
    def response(self, data, status=200):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = status
        response.read.return_value = json.dumps(data).encode()
        return response

    def test_protocol_probes_use_no_application_resolvers_or_mutations(self):
        responses = [self.response({'data': {'__typename': 'Query'}}),
                     self.response({'data': {'first': 'Query'}}),
                     self.response({'data': {'__typename': 'Query'}})]
        with patch.object(ops.OPENER, 'open', side_effect=responses) as opened:
            result = ops.probes('192.0.2.1')
        self.assertEqual(len(result), 3)
        first, second, third = [x.args[0] for x in opened.call_args_list]
        self.assertEqual(json.loads(first.data), {'query': 'query RecoveryProbe { __typename }'})
        self.assertEqual(json.loads(second.data)['variables'], {'include': False})
        self.assertEqual(third.get_method(), 'GET')
        self.assertTrue(third.full_url.startswith('http://192.0.2.1/graphql?'))
        self.assertIsNone(third.data)

    def test_errors_or_unexpected_data_are_rejected(self):
        for data in ({'errors': [{'message': 'failed'}]},
                     {'data': {'__typename': 'Other'}},
                     {'data': {'__typename': 'Query'}, 'extra': 'unexpected'}):
            with self.subTest(data=data), patch.object(ops.OPENER, 'open', return_value=self.response(data)):
                with self.assertRaisesRegex(RuntimeError, 'Unexpected GraphQL result'):
                    ops.request('192.0.2.1', 'test', {'query': '{ __typename }'}, {'__typename': 'Query'})

    def test_large_or_failed_response_is_rejected(self):
        for status, raw in ((500, b'{}'), (200, b'x' * (ops.MAX_RESPONSE + 1))):
            response = self.response({}, status)
            response.read.return_value = raw
            with patch.object(ops.OPENER, 'open', return_value=response):
                with self.assertRaisesRegex(RuntimeError, 'status or size'):
                    ops.request('192.0.2.1', 'test', {}, {})

    def test_target_selects_graphql_stack_before_any_probe(self):
        with patch.object(ops, 'target', side_effect=RuntimeError('isolation failed')) as target, \
                patch.object(ops, 'probes') as probes, \
                patch('sys.argv', ['probe', '--expected-account', 'account', '--result', '/nonexistent/result.json']):
            with self.assertRaisesRegex(RuntimeError, 'isolation failed'):
                ops.main()
        target.assert_called_once_with('account', ops.STACK)
        probes.assert_not_called()


if __name__ == '__main__':
    unittest.main()
