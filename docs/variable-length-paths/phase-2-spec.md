# Phase 2: Filtered Quantified Path Patterns

**Status:** To be filled
**Prerequisites:** Phase 1 complete

---

## Overview

Phase 2 adds filtering support to quantified path patterns. This includes edge type filters, edge property filters, and target node label/property filters. These filters are applied at each hop during traversal.

---

## Planned Features

### Edge Type Filtering
```cypher
// Filter by relationship type
MATCH (n)-[:KNOWS]->+(m)
RETURN n, m

// Multiple types (OR semantics)
MATCH (n)-[:KNOWS|FRIEND]->+(m)
RETURN n, m
```

### Edge Property Filtering
```cypher
// Property filter applies to ALL hops
MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m
```

### Target Node Filtering
```cypher
// Filter end node by label
MATCH (n)->+(m:Person)
RETURN n, m

// Filter end node by properties
MATCH (n)->+(m:Person {active: true})
RETURN n, m
```

### Combined Filters
```cypher
// Edge type + target label
MATCH (n:Person {name: 'Alice'})-[:MANAGES]->+(m:Person)
RETURN m

// Edge type + edge properties + target label
MATCH (n)-[:KNOWS {trusted: true}]->+(m:Person)
RETURN n, m
```

---

## Design Considerations

### Filtering Strategies

> [!WARNING]
> This section should be carefully considered and discussed with the team.
> Our design choices will drastically impact the implementation.

**Question:** How do we execute filters in the middle of a path?

```cypher
MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m
```

In this query, all edges that are matched should have the `:KNOWS {since: 2020}` filter applied.
In practice, it means that we cannot simply run a depth/breadth-first exploration from `n` since
we need to apply the filters at each hop during traversal.

**Approach A - Filter per hop:**
At each step of the BFS/DFS, check the edge type and properties before following it.
Simple but may be slow for property filters (requires property lookups at each hop).

**Approach B - Pre-filter edges, then traverse:**
Build a filtered edge set first, then traverse only within that set.
More memory but potentially faster for repeated traversals.

**Approach C - Hybrid:**
Use the edge type index to pre-filter by type, then check properties per hop.

### Property Filter Semantics

Property filters must match **ALL** relationships in the path:

```cypher
// Graph: (A)-[:KNOWS {since: 2020}]->(B)-[:KNOWS {since: 2021}]->(C)

MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m

// Result: (A, B) only
// Does NOT return (A, C) because the second hop has since: 2021
```

### Target Node Filter Semantics

Target node filters apply only to the final node in the path:

```cypher
// Graph: (A:Person)-[:KNOWS]->(B:Bot)-[:KNOWS]->(C:Person)

MATCH (n)-[]->+(m:Person)
RETURN n, m

// Results: (A, C) — B is traversed but not returned as it is not :Person
// Also: (B, C)
```

**Open question:** Should intermediate nodes also be checked against the target filter,
or only the final destination?

---

## Requirements

- [ ] Edge type filtering in quantified patterns
- [ ] Edge property filtering in quantified patterns
- [ ] Target node label filtering
- [ ] Target node property filtering
- [ ] Multi-type edge filters (OR semantics with `|`)
- [ ] Integration with Phase 1 edge variable return

---

## Open Questions

1. What is the performance impact of per-hop property lookups?
2. Should we support index-accelerated type filtering?
3. How do filters interact with cycle detection?
4. Should intermediate nodes be filtered or only the final target?

---

_This document will be completed after Phase 1 implementation._
