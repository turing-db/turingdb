# Join Feature Bug Report

## Summary

Created 27 unit tests to stress-test the join feature. Found multiple bugs including query execution failures and known crash patterns.

## Test Results Overview

| Category | Passed | Failed | Disabled |
|----------|--------|--------|----------|
| Total    | 20     | 3      | 4        |

---

## Confirmed Bugs

### Bug 1: HashJoinProcessor Map Comparison Error

**File**: `query/pipeline/processors/HashJoinProcessor.cpp`
**Line**: 203

```cpp
if (const auto it = findInMap(_leftMap, rightCol, i); it != _rightMap.end()) {
```

**Issue**: Searches in `_leftMap` but compares against `_rightMap.end()`. This is incorrect - the comparison should be against `_leftMap.end()`.

**Impact**: May cause incorrect hash lookup behavior, potentially returning wrong join results or missing matches.

**Fix**: Change to:
```cpp
if (const auto it = findInMap(_leftMap, rightCol, i); it != _leftMap.end()) {
```

---

### Bug 2: Query Fails on Property Name Filter

**Failing Tests**:
- `singleSourceInterest` - `WHERE b.name = 'Unique'`
- `singlePersonMultipleInterests` - `WHERE a.name = 'A'`

**Queries**:
```cypher
MATCH (a:Person)-->(b:Interest)
WHERE b.name = 'Unique'
RETURN a.name, b.name
```

```cypher
MATCH (a:Person)-->(b:Interest)
WHERE a.name = 'A'
RETURN a.name, b.name
```

**Expected**: Query should return results
**Actual**: Query returns `false` (failure)

**Impact**: Simple WHERE clause filtering on string properties fails.

---

### Bug 3: Query Fails on Multiple Property Projection

**Failing Test**: `joinWithPropertyProjection`

**Query**:
```cypher
MATCH (a:Person)-->(b:Interest)
RETURN a.name, a.isFrench, b.name
```

**Expected**: Query should return person names, isFrench flag, and interest names
**Actual**: Query returns `false` (failure)

**Impact**: Cannot project multiple properties from the same node in RETURN clause.

---

### Bug 4: MaterializeProcessor Segfault with Edge Types

**Disabled Tests** (crash when enabled):
- `DISABLED_explicitEdgeTypeCrash`
- `DISABLED_mixedEdgeTypesInPath`
- `DISABLED_cyclicPatternJoin`
- `DISABLED_symmetricRelationshipJoin`

**Crash Pattern**:
```cypher
MATCH (a:Person)-[e1:INTERESTED_IN]->(b:Interest)<-[e2:INTERESTED_IN]-(c:Person)
WHERE a <> c
RETURN a.name, b.name, c.name
```

**Location**: `MaterializeProcessor::execute()` - segfault when processing multi-hop patterns with explicit edge type labels.

**Impact**: Any query using explicit edge type labels in multi-hop patterns will crash the database.

---

## Bug-Prone Areas Identified

1. **Chunk boundary handling** in `HashJoinProcessor::execute()` - complex state machine for `_rowOffsetState`
2. **Column offset calculations** in `processRightStream()` (line 372)
3. **flushRightStream()** column indexing (line 464 uses `_leftRowLen + j` which may be incorrect when columns are skipped)

---

## Why Hash Join Tests Are Disabled

Four tests in JoinFeatureTest.cpp have been disabled (prefixed with `DISABLED_`) because they trigger a segmentation fault in the MaterializeProcessor during query execution:

| Test Name | Reason Disabled |
|-----------|-----------------|
| `DISABLED_explicitEdgeTypeCrash` | Crashes when using explicit edge types like `-[e1:INTERESTED_IN]->` in multi-hop patterns |
| `DISABLED_mixedEdgeTypesInPath` | Crashes when mixing different edge types (`-[:KNOWS]->` and `-[:INTERESTED_IN]->`) in a single path |
| `DISABLED_cyclicPatternJoin` | Crashes when matching cyclic patterns `(a)->(b)->(c)->(a)` with explicit edge types |
| `DISABLED_symmetricRelationshipJoin` | Crashes when matching symmetric relationships `(a)->(b)->(a)` with explicit edge types |

**Root Cause**: The crash occurs in `MaterializeProcessor::execute()` when the query pipeline attempts to materialize results from multi-hop patterns that include explicit edge type labels (e.g., `-[:KNOWS]->`). The bug is NOT in the HashJoinProcessor itself, but in how the MaterializeProcessor handles the output schema when edge types are explicitly specified.

**Note**: These tests can be run manually using `--gtest_also_run_disabled_tests`, but will crash the test process. They remain in the test suite as regression tests - once the MaterializeProcessor bug is fixed, they should pass and can be re-enabled by removing the `DISABLED_` prefix.

---

## Test File Location

`/home/dev/turingdb/test/query/queries/JoinFeatureTest.cpp`

## How to Run Tests

```bash
cd /home/dev/turingdb/build
make test_join_feature
./test/query/queries/test_join_feature
```

To run specific test:
```bash
./test/query/queries/test_join_feature --gtest_filter=JoinFeatureTest.megaHubJoin
```

To run disabled tests (will crash):
```bash
./test/query/queries/test_join_feature --gtest_also_run_disabled_tests
```
