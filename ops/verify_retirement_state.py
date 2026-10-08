"""Read-only verification of the serverless savings/retirement controls.

No function invocations, schedule changes, deployments or data access.
Private results contain settings and violations, never application records.
"""
import argparse
import datetime
import json
from pathlib import Path

from retire_integrations import snapshot, save

MANIFESTS = ['retire-integrations.json', 'retire-dev-execution.json', 'retire-prod-execution.json']
READER = 's3-json-to-client'


def verify(account):
    functions, rules, protected = {}, {}, {}
    for filename in MANIFESTS:
        manifest = json.loads(Path(__file__).with_name(filename).read_text())
        current = snapshot(manifest, account)
        functions.update(current['functions_before'])
        rules.update(current['rules_before'])
        protected.update(current['preserved_before'])
    violations = []
    for name, value in functions.items():
        if value != 0:
            violations.append('Retired function execution is not disabled: ' + name)
    for name, value in rules.items():
        if value != 'DISABLED':
            violations.append('Retired schedule is not disabled: ' + name)
    if READER not in protected or READER in functions:
        violations.append('Protected reader is not exclusively preserved by the manifests')
    elif protected[READER] == 0:
        violations.append('Protected reader execution is disabled')
    return dict(account=account, verified_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                functions=functions, rules=rules, reader_concurrency=protected.get(READER),
                violations=violations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--result', type=Path, required=True)
    args = parser.parse_args()
    if args.result.exists():
        parser.error('Use a new private result file')
    result = verify(args.expected_account)
    save(args.result, result)
    if result['violations']:
        print('\n'.join(result['violations']))
        raise SystemExit(1)
    print('Verified: retired execution/schedules disabled and community reader preserved. No AWS changes made.')


if __name__ == '__main__':
    main()
