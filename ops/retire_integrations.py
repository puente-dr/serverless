"""Apply a narrow, reversible retirement manifest; never deletes code or data.

Plan/state files contain operational metadata: store them outside this public repo.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

DEFAULT_MANIFEST = Path(__file__).with_name('retire-integrations.json')


def aws(region, *args):
    result = subprocess.run(['aws', *args, '--region', region, '--output', 'json', '--no-cli-pager'],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout or '{}')


def check_account(region, expected):
    if aws(region, 'sts', 'get-caller-identity')['Account'] != expected:
        raise RuntimeError('AWS account differs from expected account; no changes permitted')


def function_state(region, name):
    aws(region, 'lambda', 'get-function-configuration', '--function-name', name,
        '--query', '{State:State}')  # Confirm existence without returning environment values.
    return aws(region, 'lambda', 'get-function-concurrency', '--function-name', name).get('ReservedConcurrentExecutions')


def snapshot(manifest, account):
    region = manifest['region']
    check_account(region, account)
    if set(manifest['disabled_functions']) & set(manifest['preserved_functions']):
        raise ValueError('Manifest attempts to disable a protected function')
    rules = {n: aws(region, 'events', 'describe-rule', '--name', n)['State']
             for n in manifest['disabled_rules']}
    if any(s not in ('ENABLED', 'DISABLED') for s in rules.values()):
        raise ValueError('Unsupported rule state')
    return {'account': account, 'manifest': manifest, 'rules_before': rules,
            'functions_before': {n: function_state(region, n) for n in manifest['disabled_functions']},
            'preserved_before': {n: function_state(region, n) for n in manifest['preserved_functions']}}


def restore_one(region, kind, name, before):
    if kind == 'rule':
        aws(region, 'events', 'enable-rule' if before == 'ENABLED' else 'disable-rule', '--name', name)
    elif before is None:
        aws(region, 'lambda', 'delete-function-concurrency', '--function-name', name)
    else:
        aws(region, 'lambda', 'put-function-concurrency', '--function-name', name,
            '--reserved-concurrent-executions', str(before))


def validate_plan(plan):
    manifest = plan['manifest']
    for field, names in (('rules_before', 'disabled_rules'),
                         ('functions_before', 'disabled_functions'),
                         ('preserved_before', 'preserved_functions')):
        if set(plan[field]) != set(manifest[names]):
            raise ValueError('Plan resource scope differs from manifest')
    if set(plan['functions_before']) & set(plan['preserved_before']):
        raise ValueError('Plan attempts to change a protected function')
    if any(value not in ('ENABLED', 'DISABLED') for value in plan['rules_before'].values()):
        raise ValueError('Unsupported planned rule state')


def restore_function(region, name, before):
    restore_one(region, 'function', name, before)
    if function_state(region, name) != before:
        raise RuntimeError('Function capacity restoration not verified')


def apply(plan):
    validate_plan(plan)
    m, region = plan['manifest'], plan['manifest']['region']
    current = snapshot(m, plan['account'])
    if current != plan:
        raise RuntimeError('Configuration changed since plan; generate a fresh plan')
    changed = []
    try:
        for name, before in plan['rules_before'].items():
            changed.append(('rule', name, before))
            aws(region, 'events', 'disable-rule', '--name', name)
        for name, before in plan['functions_before'].items():
            changed.append(('function', name, before))
            aws(region, 'lambda', 'put-function-concurrency', '--function-name', name,
                '--reserved-concurrent-executions', '0')
        after = snapshot(m, plan['account'])
        if not all(v == 'DISABLED' for v in after['rules_before'].values()):
            raise RuntimeError('Rule verification failed')
        if not all(v == 0 for v in after['functions_before'].values()):
            raise RuntimeError('Function verification failed')
        if after['preserved_before'] != plan['preserved_before']:
            raise RuntimeError('Protected function changed')
        return after
    except BaseException as original:
        errors = []
        for kind, name, before in reversed(changed):
            if kind != 'function':
                continue
            try:
                restore_function(region, name, before)
            except Exception as error:
                errors.append((name, type(error).__name__))
        # A failed capacity restore must never be followed by schedule delivery.
        capacity_failed = bool(errors)
        for kind, name, before in reversed(changed):
            if kind != 'rule' or (capacity_failed and before == 'ENABLED'):
                continue
            try:
                restore_one(region, kind, name, before)
            except Exception as error:
                errors.append((name, type(error).__name__))
        if errors:
            raise RuntimeError('Partial rollback failed: ' + repr(errors)) from original
        raise


def rollback(plan):
    validate_plan(plan)
    region = plan['manifest']['region']
    check_account(region, plan['account'])
    # Other lifecycle decisions may have changed protected functions since this
    # plan was captured. Preserve their state at recovery time, not a stale value.
    protected = {n: function_state(region, n) for n in plan['preserved_before']}
    # Restore function capacity before re-enabling a schedule.
    for name, before in plan['functions_before'].items():
        restore_function(region, name, before)
    for name, before in plan['rules_before'].items():
        restore_one(region, 'rule', name, before)
    result = snapshot(plan['manifest'], plan['account'])
    expected = {**plan, 'preserved_before': protected}
    if result != expected:
        raise RuntimeError('Rollback verification differs from original state')
    return result


def save(path, data):
    # Create with restricted permissions, rather than narrowing them afterward.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
        f.write('\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'apply', 'rollback'])
    parser.add_argument('--expected-account', required=True)
    parser.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--result', type=Path)
    args = parser.parse_args()
    if args.action == 'plan':
        save(args.plan, snapshot(json.loads(args.manifest.read_text()), args.expected_account))
        print('Read-only plan saved; no resources changed.')
    else:
        if not args.result or args.result.exists():
            parser.error('--result must identify a new private output file')
        plan = json.loads(args.plan.read_text())
        if plan['account'] != args.expected_account:
            parser.error('Plan account differs from expected account')
        result = apply(plan) if args.action == 'apply' else rollback(plan)
        save(args.result, result)
        print(args.action + ': verified. No functions, storage, APIs or credentials deleted.')


if __name__ == '__main__':
    main()
