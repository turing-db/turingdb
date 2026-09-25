#!/usr/bin/env python3
"""
Regenerate the Cypher seed corpus for fuzz_query_engine from the repository's
own queries, so the seeds track the language the engine currently implements.

Sources:
  1. test/query-test-suite/tests/*.json  -- the "query" field
  2. regress/**/*.py                     -- client.query("...") / query("...") literals
  3. test/**/*.cpp                       -- Cypher-looking string literals of the unit tests

Each query is written to fuzz/corpus/cypher/q_<md5[:8]>.cypher. The hand-written
seeds (files not matching q_*.cypher) are left alone.

Usage:
    python3 fuzz/make_corpus.py [--prune] [--dry-run]

Options:
    --prune     Delete q_*.cypher seeds that no source produces any more
    --dry-run   Report what would change without writing anything
"""

import argparse
import ast
import glob
import hashlib
import json
import os
import re
import sys


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TEST_SUITE_DIR = os.path.join(REPO_ROOT, "test", "query-test-suite", "tests")
REGRESS_DIR = os.path.join(REPO_ROOT, "regress")
UNIT_TEST_DIR = os.path.join(REPO_ROOT, "test")
CORPUS_DIR = os.path.join(REPO_ROOT, "fuzz", "corpus", "cypher")

# A query is only worth a seed if it can plausibly reach the parser.
MIN_QUERY_LENGTH = 3

CYPHER_START = re.compile(
    r"^\s*(MATCH|RETURN|CREATE|DELETE|DETACH|MERGE|SET|REMOVE|WITH|UNWIND|CALL|"
    r"LOAD|SHOW|CHANGE|COMMIT|VECTOR|INSTALL|DROP|EXPLAIN|OPTIONAL|FOREACH|"
    r"LIST|S3)\b",
    re.IGNORECASE,
)

# The harness skips any query that names MERGE_DATAPARTS, so a seed carrying it is wasted.
SKIPPED_KEYWORD = re.compile(r"MERGE_DATAPARTS", re.IGNORECASE)

CPP_TOKEN = re.compile(
    r"(?P<comment>//[^\n]*|/\*.*?\*/)"
    r"|(?P<char>'(?:[^'\\\n]|\\.)*')"
    r'|(?:u8|u|U|L)?R"(?P<delimiter>[^()\\\s]{0,16})\((?P<raw>.*?)\)(?P=delimiter)"'
    r'|(?:u8|u|U|L)?"(?P<string>(?:[^"\\\n]|\\.)*)"',
    re.DOTALL,
)

CPP_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\0"}


def queries_from_test_suite():
    """Every "query" field of the query test suite fixtures."""
    found = []
    for path in sorted(glob.glob(os.path.join(TEST_SUITE_DIR, "*.json"))):
        with open(path, encoding="utf-8") as handle:
            fixture = json.load(handle)

        query = fixture.get("query")
        if query:
            found.append(query)

    return found


def string_literals(source):
    """Every string literal passed to a call named query/*query* in a python file."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue

        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if "query" not in name.lower():
            continue

        argument = node.args[0]
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            found.append(argument.value)
        elif isinstance(argument, ast.JoinedStr):
            # An f-string: keep the literal parts and drop the interpolations, which
            # is enough to seed the shape of the statement.
            parts = [p.value for p in argument.values if isinstance(p, ast.Constant)]
            found.append("".join(parts))

    return found


def queries_from_regress():
    """Every Cypher-looking literal handed to a query call in the regression tests."""
    found = []
    for path in sorted(glob.glob(os.path.join(REGRESS_DIR, "**", "*.py"), recursive=True)):
        with open(path, encoding="utf-8", errors="replace") as handle:
            source = handle.read()

        for literal in string_literals(source):
            if CYPHER_START.match(literal):
                found.append(literal)

    return found


def cpp_string_literals(source):
    """Every string literal of a C++ file, adjacent literals joined as the compiler joins them."""
    found = []
    group = None
    group_end = 0

    for token in CPP_TOKEN.finditer(source):
        adjacent = group is not None and not source[group_end:token.start()].strip()

        if token.group("comment") is not None:
            if adjacent:
                group_end = token.end()
            continue

        if token.group("char") is not None:
            continue

        if token.group("raw") is not None:
            text = token.group("raw")
        else:
            text = re.sub(r"\\(.)", lambda escape: CPP_ESCAPES.get(escape.group(1), escape.group(1)),
                          token.group("string"))

        if adjacent:
            group.append(text)
        else:
            if group is not None:
                found.append("".join(group))
            group = [text]

        group_end = token.end()

    if group is not None:
        found.append("".join(group))

    return found


def queries_from_unit_tests():
    """Every Cypher-looking string literal in the C++ unit tests."""
    found = []
    for path in sorted(glob.glob(os.path.join(UNIT_TEST_DIR, "**", "*.cpp"), recursive=True)):
        if "googletest" in path:
            continue

        with open(path, encoding="utf-8", errors="replace") as handle:
            source = handle.read()

        for literal in cpp_string_literals(source):
            if CYPHER_START.match(literal):
                found.append(literal)

    return found


def seed_name(query):
    return "q_" + hashlib.md5(query.encode("utf-8")).hexdigest()[:8] + ".cypher"


def main():
    parser = argparse.ArgumentParser(description="Regenerate the Cypher fuzz seed corpus")
    parser.add_argument("--prune", action="store_true", help="delete seeds no source produces any more")
    parser.add_argument("--dry-run", action="store_true", help="report changes without writing")
    args = parser.parse_args()

    suite = queries_from_test_suite()
    regress = queries_from_regress()
    unit_tests = queries_from_unit_tests()

    wanted = {}
    for query in suite + regress + unit_tests:
        if len(query.strip()) >= MIN_QUERY_LENGTH and not SKIPPED_KEYWORD.search(query):
            wanted[seed_name(query)] = query

    existing = {os.path.basename(p) for p in glob.glob(os.path.join(CORPUS_DIR, "q_*.cypher"))}
    added = sorted(set(wanted) - existing)
    stale = sorted(existing - set(wanted))

    print(f"test suite: {len(suite)} queries")
    print(f"regress:    {len(regress)} queries")
    print(f"unit tests: {len(unit_tests)} queries")
    print(f"corpus:     {len(existing)} generated seeds, {len(wanted)} wanted")
    print(f"            {len(added)} to add, {len(stale)} stale")

    if args.dry_run:
        return 0

    os.makedirs(CORPUS_DIR, exist_ok=True)

    for name in added:
        with open(os.path.join(CORPUS_DIR, name), "w", encoding="utf-8") as handle:
            handle.write(wanted[name])

    if args.prune:
        for name in stale:
            os.remove(os.path.join(CORPUS_DIR, name))

    total = len(glob.glob(os.path.join(CORPUS_DIR, "*.cypher")))
    print(f"corpus now holds {total} seeds")

    return 0


if __name__ == "__main__":
    sys.exit(main())
