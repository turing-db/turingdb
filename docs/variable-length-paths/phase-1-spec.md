# Phase 1: Basic Quantified Path Patterns

**Status:** Active Design
**Target:** Initial implementation

---

## Scope

Phase 1 implements basic quantified path patterns with no filtering on edges or target nodes. Only the source node may have label/property filters. Relationship variables are allowed and edges can be returned.

**What you CAN do:**
- Use quantified relationships: `->+`, `->*`, `->{n,m}`
- Return start nodes, end nodes, and edges
- Use relationship variables in quantified patterns
- Filter the source node by labels and properties
- Mix quantified and regular patterns
- Use all edge directions

**What you CANNOT do:**
- Filter by relationship type: `-[:KNOWS]->+`
- Filter by relationship properties: `-[{since: 2020}]->+`
- Filter the target node by labels or properties: `->+(m:Person)`
- Use WHERE clauses in patterns
- Use parenthesized path patterns

---

## Grammar

Phase 1 implements the following subset of Cypher's path pattern grammar:

```ebnf
pathPatternPhrase ::= [{ simplePathPattern | quantifiedPathPattern }]+

simplePathPattern ::= nodePattern
  [ { relationshipPattern | quantifiedRelationship } nodePattern ]*

nodePattern ::= "(" [variable] [nodeLabels] [properties] ")"

relationshipPattern ::=
  "<-" "[" [variable] [relationshipTypes] [properties] "]" "-"
  | "-" "[" [variable] [relationshipTypes] [properties] "]" "->"
  | "-" "[" [variable] [relationshipTypes] [properties] "]" "-"

quantifiedRelationship ::=
  quantifiedRelationshipPattern quantifier

quantifiedRelationshipPattern ::=
  "<-" "[" [variable] "]" "-"
  | "-" "[" [variable] "]" "->"
  | "-" "[" [variable] "]" "-"
  | "<-" "-"
  | "-" "->"
  | "-" "-"

quantifier ::= "*" | "+" | "{" quantifierRange "}"

quantifierRange ::=
  [ integer ] "," [ integer ]
  | integer
```

Note: regular (non-quantified) relationship patterns retain full type/property support.
Quantified relationship patterns only support an optional variable — no types or properties.

---

## Supported Quantified Patterns

All combinations of direction and quantifier are supported:

### Outgoing (Right Arrow)
```cypher
-->+          // 1 or more hops
-[e]->+       // with variable
-->*          // 0 or more hops
-[e]->*       // with variable
-->{n,m}      // between n and m hops
-[e]->{n,m}   // with variable
-->{n,}       // n or more hops
-->{,m}       // 0 to m hops
-->{n}        // exactly n hops (equivalent to {n,n})
```

### Incoming (Left Arrow)
```cypher
<--+
<-[e]-+
<--*
<-[e]-*
<--{n,m}
<-[e]-{n,m}
<--{n,}
<--{,m}
<--{n}
```

### Undirected
```cypher
--+
-[e]-+
--*
-[e]-*
--{n,m}
-[e]-{n,m}
--{n,}
--{,m}
--{n}
```

---

## Quantifier Semantics

| Syntax | Meaning | Min Hops | Max Hops | Notes |
|--------|---------|----------|----------|-------|
| `*`    | Zero or more | 0 | ∞ | Includes starting node |
| `+`    | One or more | 1 | ∞ | At least one hop |
| `{n,m}` | Between n and m | n | m | Bounded range |
| `{n,}` | At least n | n | ∞ | Lower bound only |
| `{,m}` | At most m | 0 | m | Upper bound only |
| `{n}`  | Exactly n | n | n | Precise hop count |

**Important notes:**
- `{,}` is equivalent to `*` (zero or more)
- Empty bounds default to 0 (lower) and ∞ (upper)
- All quantifiers must be non-negative integers

---

## Valid Example Queries

### Basic Usage
```cypher
// Simple one-or-more
MATCH (n)->+(m)
RETURN n, m

// Zero-or-more (includes starting node)
MATCH (n:Person {name: 'Alice'})->*(m)
RETURN m  // Returns Alice herself plus all direct/indirect neighbors

// Bounded hops
MATCH (n)->{1,4}(m)
RETURN n, m

// Exactly n hops
MATCH (n)->{3}(m)
RETURN n, m
```

### With Relationship Variables
```cypher
// Return the edges traversed
MATCH (n)-[e]->+(m)
RETURN n, e, m

// Variable on undirected edge
MATCH (a)-[e]-+(b)
RETURN a, e, b
```

### Filtering the Source Node
```cypher
// Source node with label
MATCH (n:Person)->+(m)
RETURN n, m

// Source node with properties
MATCH (n:Person {name: 'Alice'})->*(m)
RETURN m

// Source node with label, target unfiltered
MATCH (n:Person {dept: 'Engineering'})->+(m)
RETURN n.name, m.name
```

### Mixed with Regular Patterns
```cypher
// Quantified followed by regular (regular patterns keep full filtering)
MATCH (n)->*(m)-->(c:Crime)
RETURN n, m, c

// Multiple quantified segments
MATCH (a)->+(b)->+(c)
RETURN a, b, c

// Sandwiched patterns
MATCH (a)->+(b)<-[e:REPORTS_TO]-(c)
RETURN a, b, c
```

### Different Directions
```cypher
// Incoming
MATCH (manager)<-+(employee)
RETURN manager, employee

// Undirected
MATCH (person1)-+(person2)
RETURN person1, person2
```

---

## Restrictions in Phase 1

### Cannot Filter Quantified Edges by Type
```cypher
// ERROR: Edge type filters not supported on quantified patterns in Phase 1
MATCH (n)-[:KNOWS]->+(m)
RETURN n, m
```

### Cannot Filter Quantified Edges by Properties
```cypher
// ERROR: Edge property filters not supported on quantified patterns in Phase 1
MATCH (n)-[{since: 2020}]->+(m)
RETURN n, m
```

### Cannot Filter Target Nodes
```cypher
// ERROR: Target node filters not supported on quantified patterns in Phase 1
MATCH (n)->+(m:Person)
RETURN n, m
```

### Quantifiers Must Be Literals
```cypher
// ERROR: Expected literal integer
MATCH (n)->{1,$maxHops}(m)
RETURN n, m
```

### No Parenthesized Path Patterns
```cypher
// ERROR: Parenthesized patterns not supported in Phase 1
MATCH (n) ((a)-[:NEXT]->(b)){1,5} (m)
RETURN n, m
```

---

## Error Messages

When users attempt unsupported features, provide clear, actionable errors:

```
ERROR: Edge type filters not supported on quantified patterns
  MATCH (n)-[:KNOWS]->+(m) RETURN n, m
              ^
HINT: This feature is planned for Phase 2.
      For now, remove the type filter: (n)->+(m)

ERROR: Edge property filters not supported on quantified patterns
  MATCH (n)-[{since: 2020}]->+(m)
             ^
HINT: This feature is planned for Phase 2.

ERROR: Target node filters not supported on quantified patterns
  MATCH (n)->+(m:Person)
               ^
HINT: This feature is planned for Phase 2.

ERROR: Dynamic quantifiers not supported
  MATCH (n)->{1,$max}(m)
                ^
HINT: Use a literal number: ->{1,10}

ERROR: Parenthesized path patterns not yet implemented
  MATCH (n) ((a)-[:REL]->(b)){1,5} (m)
            ^
HINT: This feature is planned for Phase 3.
```

---

## Semantic Behavior

### Path Matching

A quantified pattern `(n)->{min,max}(m)` matches if there exists a path:
- Starting at node `n`
- Ending at node `m`
- Consisting of `k` relationships (where `min <= k <= max`)
- Following the specified direction

### Zero-or-More Semantics

`->*` and `->{0,}` include the starting node:

```cypher
// Graph: (A)-[:MANAGES]->(B)-[:MANAGES]->(C)

MATCH (n:Person {name: 'A'})->*(m)
RETURN m

// Results: A (0 hops), B (1 hop), C (2 hops)
```

This is useful for hierarchical queries where you want to include the root.

---

## Key Design Decisions

### Path Uniqueness
**Status: To be filled**

**Question:** Should we return multiple rows for multiple paths between the same (n, m) pair?

**Option A - One row per path:**
```
Graph: A-[:REL]->B-[:REL]->C
       A-[:REL]->D-[:REL]->C

MATCH (a)->+(c) RETURN a, c

Results:
  (A, C) from path A->B->C
  (A, C) from path A->D->C  // duplicate endpoints
```

**Option B - One row per unique (n, m) pair:**
```
Same graph and query:

Results:
  (A, C) // only once
```

**Implications:**
- Option A: More rows, reflects actual paths found
- Option B: Fewer rows, simpler results, requires deduplication

---

### Max Hop Limits
**Status: To be filled**

**Question:** What should be the default upper bound for unbounded quantifiers (`*`, `+`, `{n,}`)?

**Options:**
- No limit (risky for large graphs, potential infinite loops)
- 10 hops (conservative, may miss valid paths)
- 100 hops (permissive, covers most real-world cases)
- 1000 hops (very permissive)
- Configurable per-query or globally

**Considerations:**
- Performance: Higher limits = longer query times
- Usability: Too low = users hit limits unexpectedly
- Safety: No limit = potential runaway queries

**Decision:** _To be filled by team_

**Suggested approach:**
- Default limit: TBD (e.g., 100 hops)
- Global configuration parameter
- Future: Query-level override syntax

---

### Cycle Handling
**Status: To be filled**

**Question:** Should paths be allowed to revisit nodes?

**Option A - Allow cycles:**
```
Graph: (A)-[:REL]->(B)-[:REL]->(A)  // cycle

MATCH (a)->+(b) could match:
  A->B (1 hop)
  A->B->A (2 hops)
  A->B->A->B (3 hops)
  ... potentially infinite
```

**Option B - Forbid cycles (path cannot revisit nodes):**
```
Same graph:
MATCH (a)->+(b)

Results:
  A->B
  B->A
  (no paths that revisit nodes)
```

**Option C - Configurable:**
- Global setting
- Query-level hint
- Separate operators for cycle-allowing vs cycle-forbidding

> [!WARNING]
> Regardless of our choice, allowing cycles should be forbidden for unbounded quantifiers
> which will run infinitely in the case of cycles.

**Implications:**
- Option A: More flexible but requires max hop limit for safety
- Option B: Simpler, safer, predictable result sizes
- Option C: Most flexible but adds complexity

---

## Result Cardinality

The number of results depends on the path uniqueness decision:

**If one row per path:**
- Multiple paths between the same endpoints produce multiple rows
- Result size: O(number of paths found)

**If one row per unique (n, m) pair:**
- Endpoints are deduplicated
- Result size: O(number of reachable node pairs)

**Example:**
```
Graph: (A)-[:FRIEND]->(B)-[:FRIEND]->(C)
       (A)-[:FRIEND]->(D)-[:FRIEND]->(C)

Query: MATCH (a)->+(c) RETURN a, c
```

One row per path: 4 rows (**preferred**)
- (A, B)
- (A, C) via B
- (A, D)
- (A, C) via D

One row per endpoint pair: 3 rows
- (A, B)
- (A, C)
- (A, D)

---

## Example Test Cases

### Test 1: Basic One-or-More
```cypher
CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
       -[:KNOWS]->(c:Person {name: 'C'})

MATCH (n)->+(m)
RETURN n.name, m.name

Expected: (A,B), (A,C), (B,C)
```

### Test 2: Zero-or-More Includes Self
```cypher
MATCH (n:Person {name: 'A'})->*(m)
RETURN m.name

Expected: A, B, C
```

### Test 3: Bounded Hops
```cypher
MATCH (n)->{1,1}(m)
RETURN n.name, m.name

Expected: (A,B), (B,C)
```

### Test 4: No Path
```cypher
CREATE (isolated:Person {name: 'D'})
MATCH (n)->+(isolated)
RETURN n.name

Expected: (empty result)
```

### Test 5: Returning Edges
```cypher
CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
       -[:FOLLOWS]->(c:Person {name: 'C'})

MATCH (n)-[e]->+(m)
RETURN n.name, e, m.name

Expected: edges are returned for each path
```

### Test 6: Cycle Graph (depends on cycle handling decision)
```cypher
CREATE (a:Node)-[:REL]->(b:Node)-[:REL]->(a)

MATCH (n)->+(m)
RETURN n, m

Expected (if cycles forbidden): (a,b), (b,a)
Expected (if cycles allowed): (a,b), (b,a), (a,b,a), (b,a,b), ...
```

### Test 7: Mixed with Regular Patterns
```cypher
MATCH (a)->+(b)-[:LIVES_IN]->(c:City)
RETURN a.name, c.name

Expected: All people transitively connected who live in cities
```

### Test 8: Multiple Quantified Patterns
```cypher
MATCH (a)->+(b)->+(c)
RETURN a, b, c

Expected: Paths with both quantified segments satisfied
```

### Test 9: Source Node Filter Only
```cypher
MATCH (n:Person {name: 'A'})->+(m)
RETURN m.name

Expected: All nodes reachable from A via any edges
```

---

## Next Steps

1. **Make key design decisions** (path uniqueness, cycle handling, max hops)
2. **Review [Implementation Notes](implementation-notes.md)** for execution algorithms
3. **Finalize AST design** based on parser considerations
4. **Set up test infrastructure** following [Testing Strategy](testing-strategy.md)
5. **Begin parser implementation**

---

## See Also

- [Implementation Notes](implementation-notes.md) - Execution algorithms and optimizations
- [Testing Strategy](testing-strategy.md) - Test coverage approach
- [Overview](overview.md) - General context and phase breakdown
