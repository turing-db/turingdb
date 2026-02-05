# Testing Strategy

This document outlines the testing approach for quantified path patterns implementation.

---

## Test Categories

### 1. Unit Tests
Parser, AST, and core components

### 2. Integration Tests
End-to-end query execution

### 3. Performance Tests
Large graphs and complex patterns

### 4. Regression Tests
Neo4j compatibility validation

### 5. Error Handling Tests
Invalid syntax and unsupported features

---

## Unit Tests

### Parser Tests

**Valid syntax recognition:**
```
Test: Parse quantifiers
Input: "->+", "->*", "->{1,5}", "->{,10}", "->{5,}"
Expected: Valid AST nodes with correct quantifier representation

Test: Parse all directions
Input: "->+", "<-+", "-+"
Expected: Correct direction in AST (RIGHT, LEFT, BOTH)

Test: Parse relationship types
Input: "-[:KNOWS]->+", "-[:KNOWS|FRIEND]->*"
Expected: Types correctly captured in AST

Test: Parse properties
Input: "-[:KNOWS {since: 2020}]->+"
Expected: Properties correctly parsed
```

**Invalid syntax rejection:**
```
Test: Reject relationship variables in quantified patterns
Input: "-[r:KNOWS]->+"
Expected: Parse error with helpful message

Test: Reject legacy syntax
Input: "-[*1..5]->"
Expected: Parse error explaining not supported

Test: Reject invalid quantifier ranges
Input: "->{5,3}", "->{-1,5}"
Expected: Parse error (min > max, negative values)
```

### AST Construction Tests

```
Test: Quantifier representation
Input: "->+"
Expected: Quantifier(ONE_OR_MORE, min=1, max=null)

Test: Quantifier representation
Input: "->{3,7}"
Expected: Quantifier(RANGE, min=3, max=7)

Test: Direction representation
Input: "<-{2,}"
Expected: Direction=LEFT, Quantifier(RANGE, min=2, max=null)
```

---

## Integration Tests

### Basic Traversal

**Test: Simple one-or-more**
```
Graph:
  CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
         -[:KNOWS]->(c:Person {name: 'C'})

Query:
  MATCH (n)-[:KNOWS]->+(m)
  RETURN n.name, m.name
  ORDER BY n.name, m.name

Expected:
  A, B
  A, C
  B, C
```

**Test: Zero-or-more includes self**
```
Query:
  MATCH (n:Person {name: 'A'})-[:KNOWS]->*(m)
  RETURN m.name
  ORDER BY m.name

Expected:
  A    // 0 hops
  B    // 1 hop
  C    // 2 hops
```

**Test: Exactly n hops**
```
Query:
  MATCH (n)-[:KNOWS]->{2}(m)
  RETURN n.name, m.name

Expected:
  A, C   // A->B->C (exactly 2 hops)
```

**Test: Bounded range**
```
Query:
  MATCH (n)-[:KNOWS]->{1,2}(m)
  RETURN n.name, m.name
  ORDER BY n.name, m.name

Expected:
  A, B   // 1 hop
  A, C   // 2 hops
  B, C   // 1 hop
```

### Direction Tests

**Test: Incoming relationships**
```
Graph:
  CREATE (a)-[:REPORTS_TO]->(b)-[:REPORTS_TO]->(c)

Query:
  MATCH (manager)<-[:REPORTS_TO]+(employee)
  WHERE manager.name = 'C'
  RETURN employee.name
  ORDER BY employee.name

Expected:
  A
  B
```

**Test: Undirected relationships**
```
Graph:
  CREATE (a:Person)-[:FRIEND]-(b:Person)-[:FRIEND]-(c:Person)

Query:
  MATCH (n:Person {name: 'A'})-[:FRIEND]-(m)
  RETURN m.name

Expected:
  B
  C
```

### Multiple Relationship Types

**Test: OR semantics**
```
Graph:
  CREATE (a)-[:KNOWS]->(b)-[:FRIEND]->(c)

Query:
  MATCH (n)-[:KNOWS|FRIEND]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  B
  C
```

### Property Filtering

**Test: All hops must match property**
```
Graph:
  CREATE (a)-[:KNOWS {since: 2020}]->(b)
         -[:KNOWS {since: 2021}]->(c)

Query:
  MATCH (n)-[:KNOWS {since: 2020}]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  B   // Only B, because A->C has since: 2021 in second hop
```

**Test: Empty property map matches all**
```
Graph:
  CREATE (a)-[:KNOWS {since: 2020}]->(b)
         -[:KNOWS {since: 2021}]->(c)

Query:
  MATCH (n)-[:KNOWS]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  B
  C   // Matches both
```

### Edge Cases

**Test: No matching paths**
```
Graph:
  CREATE (a:Person {name: 'A'})
  CREATE (b:Person {name: 'B'})  // disconnected

Query:
  MATCH (n)-[:KNOWS]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  (empty result)
```

**Test: Self-loop**
```
Graph:
  CREATE (a:Person)-[:KNOWS]->(a)

Query:
  MATCH (n)-[:KNOWS]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  (depends on cycle handling decision)
  If cycles forbidden: (empty result)
  If cycles allowed: A
```

**Test: Max hop limit reached**
```
Graph:
  CREATE (a)-[:NEXT]->(b)-[:NEXT]->(c)-[:NEXT]-> ... (1000 nodes)

Query with max_hops = 100:
  MATCH (a)-[:NEXT]->+(m)
  RETURN count(m)

Expected:
  100  // or emit warning about limit reached
```

### Mixed Patterns

**Test: Quantified + regular pattern**
```
Graph:
  CREATE (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:LIVES_IN]->(d:City)
  CREATE (e)-[:KNOWS]->(f)  // no city

Query:
  MATCH (n)-[:KNOWS]->+(m)-[:LIVES_IN]->(c:City)
  WHERE n.name = 'A'
  RETURN m.name, c.name

Expected:
  C, D
```

**Test: Multiple quantified segments**
```
Graph:
  CREATE (a)-[:FRIEND]->(b)-[:FRIEND]->(c)
         -[:COLLEAGUE]->(d)-[:COLLEAGUE]->(e)

Query:
  MATCH (a)-[:FRIEND]->+(b)-[:COLLEAGUE]->+(c)
  RETURN a.name, b.name, c.name

Expected:
  A, C, D
  A, C, E
  ... (all valid combinations)
```

### Wildcard Relationship Type

**Test: Empty brackets match any type**
```
Graph:
  CREATE (a)-[:KNOWS]->(b)-[:FRIEND]->(c)

Query:
  MATCH (n)-[]->+(m)
  WHERE n.name = 'A'
  RETURN m.name

Expected:
  B
  C
```

---

## Cycle Handling Tests

**Test: Simple cycle**
```
Graph:
  CREATE (a)-[:REL]->(b)-[:REL]->(a)

Query:
  MATCH (n)-[:REL]->+(m)
  RETURN n, m

Expected (if cycles forbidden):
  (A, B)
  (B, A)

Expected (if cycles allowed with limit):
  (A, B)
  (B, A)
  (A, B, A)  // if max >= 2
  ...
```

**Test: Complex cycle**
```
Graph:
  CREATE (a)-[:REL]->(b)-[:REL]->(c)-[:REL]->(a)

Query:
  MATCH (n)-[:REL]->+(m)
  RETURN count(*)

Expected:
  (depends on cycle policy and max hop limit)
```

---

## Performance Tests

### Large Graph Performance

**Test: High branching factor**
```
Graph:
  CREATE (hub:Person)
  FOR i IN range(1, 10000):
    CREATE (hub)-[:KNOWS]->(:Person)

Query:
  MATCH (hub)-[:KNOWS]->+(m)
  WHERE hub.name = 'Hub'
  RETURN count(m)

Expected:
  Should complete within reasonable time
  Monitor memory usage
  Check frontier size warnings
```

**Test: Deep linear path**
```
Graph:
  CREATE chain of 1000 nodes connected linearly

Query:
  MATCH (start)-[:NEXT]->+(end)
  WHERE start.name = 'Node_0'
  RETURN count(end)

Expected:
  999 or capped at max_hops
  Should be fast (O(n))
```

**Test: Disconnected components**
```
Graph:
  CREATE 100 separate components of 1000 nodes each

Query:
  MATCH (n)-[:REL]->+(m)
  RETURN count(*)

Expected:
  Should not attempt cross-component traversal
  Performance should scale linearly with component size
```

### Batch Processing Performance

**Test: Many seeds with shared paths**
```
Graph:
  CREATE (a)-[:REL]->(common)-[:REL]->(b)
         (c)-[:REL]->(common)
         (d)-[:REL]->(common)

Seeds: [A, C, D]

Query:
  MATCH (n)-[:REL]->+(m)
  WHERE n IN ['A', 'C', 'D']
  RETURN n, m

Expected:
  Should reuse expansion from 'common'
  Monitor shared work percentage
```

**Test: Batch with varying path lengths**
```
Seeds: [Shallow1, Medium1, Deep1, ...]

Expected:
  Should balance load across workers
  No worker should be idle while others work
```

---

## Error Handling Tests

### Syntax Errors

**Test: Invalid quantifier**
```
Query: MATCH (n)-[:REL]->{5,3}(m)
Expected: ERROR: Invalid quantifier range: min (5) > max (3)

Query: MATCH (n)-[:REL]->{-1,5}(m)
Expected: ERROR: Quantifier values must be non-negative
```

### Unsupported Features

**Test: Relationship variable**
```
Query: MATCH (n)-[r]->+(m) RETURN r
Expected: ERROR with hint about Phase 2

Query: MATCH (n)-[r:KNOWS]->+(m)
Expected: ERROR: Remove variable name
```

**Test: Dynamic quantifier**
```
Query: MATCH (n)-[:KNOWS]->{1,$max}(m)
Expected: ERROR: Dynamic quantifiers not supported in Phase 1
```

**Test: Parenthesized pattern**
```
Query: MATCH ((a)-[:REL]->(b)){1,5}
Expected: ERROR: Planned for Phase 3
```

**Test: WHERE clause in pattern**
```
Query: MATCH (n)-[:REL]->+ WHERE n.age > 18 (m)
Expected: ERROR: Planned for Phase 4
```

---

## Regression Tests

### Neo4j Compatibility

Run identical queries on both systems and compare results:

**Test suite:**
```
1. Basic quantified patterns
2. All quantifiers: +, *, {n,m}, {n,}, {,m}, {n}
3. All directions
4. Property filters
5. Multiple relationship types
6. Mixed patterns

For each test:
  - Run on Neo4j
  - Run on our system
  - Compare result sets (may need sorting)
  - Validate cardinality matches
```

**Known divergences:**
- Path uniqueness semantics (if we differ)
- Cycle handling (if we differ)
- Max hop limits (may differ)

Document any intentional differences.

---

## Test Data Sets

### Small Test Graphs

**Linear chain:**
```
(A)->(B)->(C)->(D)->(E)
```

**Binary tree:**
```
       A
      / \
     B   C
    / \ / \
   D  E F  G
```

**Cycle:**
```
(A)->(B)->(C)->(A)
```

**Disconnected:**
```
(A)->(B)   (C)->(D)
```

**Hub and spoke:**
```
  B   C   D
   \ | /
     A
   / | \
  E   F   G
```

### Medium Test Graphs

- Social network: 10K nodes, avg degree 50
- Citation network: 100K nodes, power-law degree
- Road network: 50K nodes, low degree (2-4)

### Large Test Graphs

- LDBC Social Network Benchmark data
- Real-world graph: Wikipedia link graph subset
- Synthetic: Erdős-Rényi with varying parameters

---

## Coverage Goals

### Parser Coverage
- ✅ All quantifier syntaxes
- ✅ All direction combinations
- ✅ Relationship types and properties
- ✅ Invalid syntax rejection

### Execution Coverage
- ✅ All quantifier types in execution
- ✅ All graph topologies (linear, tree, cycle, disconnected)
- ✅ Edge cases (empty graph, single node, self-loops)
- ✅ Property filtering correctness

### Performance Coverage
- ✅ Small graphs (< 1K nodes)
- ✅ Medium graphs (1K - 100K nodes)
- ✅ Large graphs (> 100K nodes)
- ✅ High-degree nodes (hubs)
- ✅ Deep paths (> 100 hops)

### Error Coverage
- ✅ All syntax errors
- ✅ All unsupported features
- ✅ Resource limits (memory, time)

---

## Test Automation

### Continuous Integration

**On every commit:**
- Run all unit tests
- Run small integration tests
- Run syntax error tests

**Nightly:**
- Run full integration test suite
- Run medium-size performance tests
- Generate coverage reports

**Weekly:**
- Run large-scale performance tests
- Run Neo4j regression tests
- Memory profiling

### Test Organization

```
tests/
  unit/
    parser/
      test_quantifiers.cpp
      test_directions.cpp
      test_properties.cpp
    ast/
      test_ast_construction.cpp
  integration/
    basic/
      test_one_or_more.cpp
      test_zero_or_more.cpp
      test_bounded.cpp
    advanced/
      test_mixed_patterns.cpp
      test_property_filters.cpp
    cycles/
      test_simple_cycle.cpp
      test_complex_cycle.cpp
  performance/
    test_large_graph.cpp
    test_high_degree.cpp
    test_batch_processing.cpp
  regression/
    neo4j_compatibility/
      test_basic_patterns.cpp
      test_quantifiers.cpp
  error/
    test_syntax_errors.cpp
    test_unsupported_features.cpp
```

---

## Acceptance Criteria

**Parser:**
- ✅ Parses all valid quantified patterns
- ✅ Rejects invalid syntax with helpful errors
- ✅ Rejects unsupported Phase 2/3/4 features

**Execution:**
- ✅ Returns correct results for all test cases
- ✅ Handles cycles according to policy
- ✅ Respects max hop limits
- ✅ Handles empty results correctly

**Performance:**
- ✅ Completes small queries (< 1K nodes) in < 100ms
- ✅ Completes medium queries (< 100K nodes) in < 5s
- ✅ Does not crash on large queries
- ✅ Emits warnings for potential performance issues

**Error Handling:**
- ✅ All errors have clear, actionable messages
- ✅ Hints provided for unsupported features
- ✅ Graceful degradation on resource limits

---

## See Also

- [Phase 1 Specification](phase-1-spec.md) - Feature requirements
- [Implementation Notes](implementation-notes.md) - Execution algorithms
- [Overview](overview.md) - General context
