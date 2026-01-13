# Join Query Engine Bugs

This document summarizes bugs discovered in the query engine through the JoinFeatureTest suite.

## Summary

21 out of 50 tests fail due to query engine bugs. The tests correctly compute expected results using the GraphReader API and compare against actual query results.

## Bug Categories

### 1. Column Value Corruption

The query engine returns values from wrong node types in result columns.

**Example from `tripleHopJoin`:**
- Query: `MATCH (a:Person)-->(b:Interest)-->(c:Category)`
- Expected `b` (Interest) values: `Shared`, `Unique`, `MegaHub`
- Actual `b` values include: `TechCorp` (Company), `Paris` (City), `B` (Person name)

**Example from `megaHubJoin`:**
- Query: `MATCH (a:Person)-->(b:Interest)<--(c:Person) WHERE b.name = 'MegaHub'`
- Expected `b` value: `MegaHub`
- Actual `b` values include: `B`, `C` (Person names instead of Interest names)

### 2. JOIN Iteration Failure

The query engine only iterates the first value for left-side join variables instead of all valid values.

**Example from `sharedInterestJoin`:**
```
Query: MATCH (a:Person)-->(b:Interest)<--(c:Person) WHERE b.name = 'Shared'

Expected (6 rows):              Actual (6 rows):
(C, Shared, B)                  (A, MegaHub, C)  ← all have a=A
(B, Shared, C)                  (A, Unique, B)   ← all have a=A
(C, Shared, A)                  (A, Unique, C)   ← all have a=A
(A, Shared, C)                  (A, Shared, B)   ← all have a=A
(B, Shared, A)                  (A, MegaHub, B)  ← all have a=A
(A, Shared, B)                  (A, Shared, C)   ← all have a=A
```

Persons B and C should appear as `a`, but only A appears.

### 3. WHERE Clause Not Applied

Filter predicates in WHERE clauses are ignored.

**Example from `sharedInterestJoin`:**
- Query includes: `WHERE b.name = 'Shared'`
- Expected: Only rows where interest is "Shared"
- Actual: Returns rows with `MegaHub`, `Unique` as interest names

### 4. Invalid Pattern Matches

The query engine returns results that don't match the MATCH pattern structure.

**Example from `eightJoinChain`:**
- Query: 8-hop chain pattern requiring 4 distinct interests
- Graph only has 3 interests with category connections
- Expected: 0 rows (no valid paths exist)
- Actual: 706 rows with invalid data

## Failing Tests

| Test Name | Expected Size | Actual Size | Issue |
|-----------|---------------|-------------|-------|
| sharedInterestJoin | 6 | 6 | Wrong values, only a=A |
| megaHubJoin | 20 | 20 | Wrong values, only a=A |
| tripleHopJoin | 14 | 14 | Corrupted column values |
| multiJoin_categoryDoubleHop | 11 | 11 | Missing persons E,D,C |
| multiJoin_tripleJoinChain | 102 | 123 | Extra invalid rows |
| multiJoin_threeWayInterestShare | 26 | 20 | Missing rows |
| multiJoin_fiveHopChain | 212 | 227 | Extra invalid rows |
| multiJoin_fourPersonChain | 252 | 519 | Extra invalid rows |
| multiJoin_sevenNodeChain | 444 | 812 | Extra invalid rows |
| multiJoin_eightJoinChain | 0 | 706 | All rows invalid |
| multiJoin_extendedChainWithCategory | 12 | 64 | Extra invalid rows |
| multiJoin_chainWithPropertyFilter | 22 | 26 | Extra invalid rows |
| multiJoin_personChainToCategory | 30 | 53 | Extra invalid rows |
| multiJoin_categoryBridge | 8 | 32 | Extra invalid rows |
| multiJoin_doubleInterestConnection | 24 | 34 | Extra invalid rows |
| commaMatch_twoPathsPropertyJoin | 15 | 7 | Missing rows |
| commaMatch_twoIndependentPaths | - | - | Missing rows |
| commaMatch_pathAndCartesianMixed | - | - | Missing rows |
| commaMatch_threePathsCombined | - | - | Missing rows |
| complexBooleanFilter | - | - | Missing rows |
| largeResultMultiChunk | - | - | Missing rows |

## Test Graph Structure

The test graph contains:
- **Persons**: A, B, C, D, E, F (with `isFrench` boolean property)
- **Interests**: Shared, Unique, MegaHub, Orphan, (one with null name)
- **Categories**: Cat1, Cat2, Cat3
- **Cities**: Paris, London, Berlin
- **Companies**: TechCorp, DataInc, CloudLtd

Key edges:
- A, B, C → Shared (INTERESTED_IN)
- A → Unique (INTERESTED_IN)
- A, B, C, D, E → MegaHub (INTERESTED_IN)
- Shared → Cat1 (BELONGS_TO)
- Unique → Cat2 (BELONGS_TO)
- MegaHub → Cat2, Cat3 (BELONGS_TO)

## Notes

- Tests are correctly implemented using GraphReader API to compute expected results
- The 29 passing tests cover simpler patterns (single joins, cartesian products, empty results)
- Edge uniqueness is NOT enforced in our Cypher subset (same edge can be traversed multiple times)
