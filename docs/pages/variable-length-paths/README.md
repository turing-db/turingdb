# Cypher Quantified Path Patterns

---

## Overview

This documentation covers the implementation of Cypher quantified path patterns in our graph DBMS. We are implementing the modern quantified relationship syntax and explicitly **not** supporting Neo4j's legacy variable-length pattern syntax (`-[*]->`).

For a general overview of what Cypher supports and our implementation approach, see **[Overview](overview.md)**.

---

## Implementation Phases

Our implementation is split into four phases, each building on the previous:

### [Phase 1: Basic Quantified Path Patterns](phase-1-spec.md)
Basic support for quantified relationships without the ability to return intermediate relationships or nodes.
Returning relationships is planned for a future phase since it requires list/array type support in the query pipeline.

**Features:**
- Quantified relationships: `->+`, `->*`, `->{n,m}`
- All edge directions: `->`, `<-`, `-`
- Relationship type and property filters
- Node return support

**Example queries:**
```cypher
MATCH (n)-[:KNOWS]->+(m) RETURN n, m
MATCH (n)-[:KNOWS]->{1,4}(m)-->(c:Crime) RETURN n, m, c
```

**Restrictions:**
- Cannot return relationships from quantified patterns
- No relationship variables in quantified patterns
- Literal quantifiers only (no expressions): This is a Cypher limitation
- No parenthesized patterns: `(n) ((:Person)-->+()) (m)`

---

### [Phase 2: Relationship Return Support](phase-2-spec.md)

> [!WARNING]
> The specs of this phase are to be filled

Adds the ability to return relationships from quantified patterns as lists.

**Requires:**
- List/array type support in type system
- Relationship variable binding in quantified patterns

**Example queries:**
```cypher
MATCH (n)-[r]->+(m) RETURN n, r, m  -- r is a list of relationships
```

---

### [Phase 3: Parenthesized Quantified Patterns](phase-3-spec.md)

> [!WARNING]
> The specs of this phase are to be filled

Support for `((pattern)){n,m}` with variable bindings and intermediate node returns.

**Example queries:**
```cypher
MATCH (start) ((n:Stop)-[:NEXT]->(m:Stop)){1,10} (end)
RETURN n, m  -- n and m are lists
```

---

## Key Design Decisions

The following critical decisions need to be made before Phase 1 implementation:

### Path Uniqueness
**To be filled** - See [Phase 1 Specification](phase-1-spec.md#path-uniqueness)

Should we return multiple rows for multiple paths between the same (n, m) pair, or deduplicate to one row per unique endpoint pair?

---

### Max Hop Limits
**To be filled** - See [Phase 1 Specification](phase-1-spec.md#max-hop-limits)

What should be the default upper bound for unbounded quantifiers (`*`, `+`, `{n,}`)?

---

### Cycle Handling
**To be filled** - See [Phase 1 Specification](phase-1-spec.md#cycle-handling)

Should paths be allowed to revisit nodes, and if so, under what conditions?

---

## Additional Documentation

- **[Implementation Notes](implementation-notes.md)** - Execution algorithms and optimization strategies
- **[Testing Strategy](testing-strategy.md)** - Test coverage and validation approach

---

## References

- [Neo4j Cypher Manual - Quantified Relationships](https://neo4j.com/docs/cypher-manual/current/patterns/reference/#quantified-relationships)
- [Neo4j Cypher Manual - Variable-Length Patterns](https://neo4j.com/docs/cypher-manual/current/patterns/variable-length-patterns/) (what we're NOT implementing)
- [GQL Standard](https://www.gqlstandards.org/)
