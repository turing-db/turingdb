#!/usr/bin/env python3
"""
review_consensus.py - Find consensus violations across multiple review passes.

Only outputs violations that appear in at least --threshold passes.
Two violations "match" when they refer to the same file, nearby lines (+-3),
and similar rule descriptions.
"""

import sys
import re
import argparse


def parse_table(text):
    """Parse a markdown violation table into a list of dicts."""
    violations = []
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line.startswith('|'):
            continue
        parts = [p.strip() for p in line.split('|')[1:-1]]
        if len(parts) < 4:
            continue
        if parts[0] == 'File' or set(parts[0]) <= {'-', ' ', ':'}:
            continue
        try:
            line_num = int(re.search(r'\d+', parts[1]).group())
        except (AttributeError, ValueError):
            line_num = 0
        violations.append({
            'file': parts[0].strip(),
            'line': line_num,
            'rule': parts[2].strip(),
            'violation': parts[3].strip(),
        })
    return violations


def rule_words(rule):
    """Extract normalized keyword set from a rule description."""
    return set(re.findall(r'[a-z]+', rule.lower()))


def violations_match(a, b):
    """Check if two violations refer to the same issue."""
    if a['file'] != b['file']:
        return False
    if abs(a['line'] - b['line']) > 3:
        return False
    wa = rule_words(a['rule'])
    wb = rule_words(b['rule'])
    if not wa or not wb:
        return True
    jaccard = len(wa & wb) / len(wa | wb)
    return jaccard > 0.3


def find_consensus(all_pass_violations, threshold):
    """Return violations appearing in at least `threshold` distinct passes."""
    # Build flat candidate list with pass provenance
    candidates = []
    for pass_num, pass_violations in enumerate(all_pass_violations):
        for v in pass_violations:
            candidates.append((pass_num, v))

    if not candidates:
        return []

    # Cluster matching violations across passes
    n = len(candidates)
    used = [False] * n
    survivors = []

    for i in range(n):
        if used[i]:
            continue
        group_passes = {candidates[i][0]}
        used[i] = True
        rep = candidates[i][1]

        for j in range(i + 1, n):
            if used[j]:
                continue
            if violations_match(rep, candidates[j][1]):
                group_passes.add(candidates[j][0])
                used[j] = True

        if len(group_passes) >= threshold:
            survivors.append(rep)

    return survivors


def main():
    parser = argparse.ArgumentParser(
        description='Find consensus violations across review passes.'
    )
    parser.add_argument('files', nargs='+',
                        help='Files containing violation tables')
    parser.add_argument('--threshold', type=int, default=2,
                        help='Minimum passes for a violation to survive')
    args = parser.parse_args()

    all_pass_violations = []
    for filepath in args.files:
        with open(filepath) as f:
            text = f.read()
        if 'NO_VIOLATIONS' in text:
            all_pass_violations.append([])
        else:
            all_pass_violations.append(parse_table(text))

    survivors = find_consensus(all_pass_violations, args.threshold)

    if not survivors:
        print('NO_VIOLATIONS')
    else:
        print('| File | Line | Rule | Violation |')
        print('|------|------|------|-----------|')
        for v in survivors:
            f = v['file']
            l = v['line']
            r = v['rule']
            d = v['violation']
            print(f'| {f} | {l} | {r} | {d} |')


if __name__ == '__main__':
    main()
