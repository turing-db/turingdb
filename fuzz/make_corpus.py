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

The hand-written seeds are also written to fuzz/corpus/http/cypher_<name>.raw for
fuzz_http_parser, each wrapped in the POST /query request the Python HTTP client
sends. The parser never reads inside the body, so the generated queries are not
wrapped: they would add thousands of seeds that differ only in Content-Length.

Usage:
    python3 fuzz/make_corpus.py [--prune] [--dry-run]

Options:
    --prune     Delete q_*.cypher and cypher_*.raw seeds that no source produces any more
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
HTTP_CORPUS_DIR = os.path.join(REPO_ROOT, "fuzz", "corpus", "http")
PROTO_CORPUS_DIR = os.path.join(REPO_ROOT, "fuzz", "corpus", "proto")

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

# Header for header what python/turingdb/http_client.py sends through httpx 0.28.
HTTP_REQUEST = (
    "POST /query?{params} HTTP/1.1\r\n"
    "Host: localhost:6666\r\n"
    "Accept-Encoding: gzip, deflate\r\n"
    "Connection: keep-alive\r\n"
    "User-Agent: python-httpx/0.28.1\r\n"
    "Accept: application/json\r\n"
    "Content-Type: application/json\r\n"
    "{authorization}"
    "Content-Length: {length}\r\n"
    "\r\n"
)

# On main, inside a change, on a commit, and with a token.
HTTP_CLIENT_STATES = [
    ("graph=default", ""),
    ("graph=default&change=31", ""),
    ("graph=default&commit=0123456789abcdef", ""),
    ("graph=default", "Authorization: Bearer token\r\n"),
]

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


def requests_from_seeds(template, states):
    """Every hand-written Cypher seed as a /query request, the client states taken in turn."""
    seeds = sorted(p for p in glob.glob(os.path.join(CORPUS_DIR, "*.cypher"))
                   if not os.path.basename(p).startswith("q_"))

    found = {}
    for index, path in enumerate(seeds):
        with open(path, "rb") as handle:
            query = handle.read()

        params, authorization = states[index % len(states)]
        header = template.format(params=params, authorization=authorization, length=len(query))
        name = "cypher_" + os.path.splitext(os.path.basename(path))[0] + ".raw"
        found[name] = header.encode("ascii") + query

    return found


def proto_queries_from_seeds():
    """Every hand-written Cypher seed as a plain query. The proto harness feeds the query to
    net::proto::TuringClient, which does the framing, so the seed is the query itself."""
    seeds = sorted(p for p in glob.glob(os.path.join(CORPUS_DIR, "*.cypher"))
                   if not os.path.basename(p).startswith("q_"))

    found = {}
    for path in seeds:
        with open(path, "rb") as handle:
            found[os.path.basename(path)] = handle.read()

    return found


def changed_seeds(corpus_dir, wanted):
    """The wanted seeds that are missing or whose bytes differ from the file on disk."""
    changed = []
    for name, request in sorted(wanted.items()):
        path = os.path.join(corpus_dir, name)
        if not os.path.exists(path):
            changed.append(name)
            continue

        with open(path, "rb") as handle:
            if handle.read() != request:
                changed.append(name)

    return changed


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

    http_wanted = requests_from_seeds(HTTP_REQUEST, HTTP_CLIENT_STATES)
    http_existing = {os.path.basename(p) for p in glob.glob(os.path.join(HTTP_CORPUS_DIR, "cypher_*.raw"))}
    http_changed = changed_seeds(HTTP_CORPUS_DIR, http_wanted)
    http_stale = sorted(http_existing - set(http_wanted))

    print(f"http:       {len(http_existing)} query requests, {len(http_wanted)} wanted")
    print(f"            {len(http_changed)} to write, {len(http_stale)} stale")

    proto_wanted = proto_queries_from_seeds()
    proto_existing = {os.path.basename(p) for p in glob.glob(os.path.join(PROTO_CORPUS_DIR, "*"))}
    proto_changed = changed_seeds(PROTO_CORPUS_DIR, proto_wanted)
    proto_stale = sorted(proto_existing - set(proto_wanted))

    print(f"proto:      {len(proto_existing)} queries, {len(proto_wanted)} wanted")
    print(f"            {len(proto_changed)} to write, {len(proto_stale)} stale")

    if args.dry_run:
        return 0

    os.makedirs(CORPUS_DIR, exist_ok=True)

    for name in added:
        with open(os.path.join(CORPUS_DIR, name), "w", encoding="utf-8") as handle:
            handle.write(wanted[name])

    if args.prune:
        for name in stale:
            os.remove(os.path.join(CORPUS_DIR, name))

    os.makedirs(HTTP_CORPUS_DIR, exist_ok=True)

    for name in http_changed:
        with open(os.path.join(HTTP_CORPUS_DIR, name), "wb") as handle:
            handle.write(http_wanted[name])

    if args.prune:
        for name in http_stale:
            os.remove(os.path.join(HTTP_CORPUS_DIR, name))

    os.makedirs(PROTO_CORPUS_DIR, exist_ok=True)

    for name in proto_changed:
        with open(os.path.join(PROTO_CORPUS_DIR, name), "wb") as handle:
            handle.write(proto_wanted[name])

    if args.prune:
        for name in proto_stale:
            os.remove(os.path.join(PROTO_CORPUS_DIR, name))

    total = len(glob.glob(os.path.join(CORPUS_DIR, "*.cypher")))
    http_total = len(os.listdir(HTTP_CORPUS_DIR))
    proto_total = len(os.listdir(PROTO_CORPUS_DIR))
    print(f"corpus now holds {total} seeds")
    print(f"http corpus now holds {http_total} seeds")
    print(f"proto corpus now holds {proto_total} seeds")

    return 0


if __name__ == "__main__":
    sys.exit(main())
