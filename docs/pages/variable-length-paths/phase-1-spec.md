# Phase 1: Basic Quantified Path Patterns

**Status:** Active Design  
**Target:** Initial implementation

---

## Scope

Phase 1 implements basic quantified path patterns without the ability to return relationships or use advanced features.

**What you CAN do:**
- Use quantified relationships: `->+`, `->*`, `->{n,m}`
- Return start and end nodes
- Filter by relationship types and properties
- Mix quantified and regular patterns
- Use all edge directions

**What you CANNOT do:**
- Return relationships from quantified patterns
- Use relationship variables in quantified patterns
- Use parenthesized path patterns
- Use WHERE clauses in patterns

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
  relationshipPatternNoVariable quantifier

relationshipPatternNoVariable ::=
  "<-" "[" [relationshipTypes] [properties] "]"
  | "-" "[" [relationshipTypes] [properties] "]" "->"
  | "-" "[" [relationshipTypes] [properties] "]"

quantifier ::= "*" | "+" | "{" quantifierRange "}"

quantifierRange ::= 
  [ integer ] "," [ integer ]
  | integer
```

---

## Supported Quantified Patterns

All combinations of direction and quantifier are supported:

### Outgoing (Right Arrow)
```cypher
->+          // 1 or more hops
-[]->+
->*          // 0 or more hops
-[]->*
->{n,m}      // between n and m hops
-[]->{n,m}
->{n,}       // n or more hops
-[]->{n,}
->{,m}       // 0 to m hops
-[]->{,m}
->{n}        // exactly n hops (equivalent to {n,n})
-[]->{n}
```

### Incoming (Left Arrow)
```cypher
<-+
<-[]-+
<-*
<-[]-*
<-{n,m}
<-[]-{n,m}
<-{n,}
<-[]-{n,}
<-{,m}
<-[]-{,m}
<-{n}
<-[]-{n}
```

### Undirected
```cypher
--+
--[]-+
--*
--[]-*
--{n,m}
--[]-{n,m}
--{n,}
--[]-{n,}
--{,m}
--[]-{,m}
--{n}
--[]-{n}
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
MATCH (n)-[:KNOWS]->+(m)
RETURN n, m

// Zero-or-more (includes starting node)
MATCH (n:Person {name: 'Alice'})-[:MANAGES]->*(m)
RETURN m  // Returns Alice herself plus all direct/indirect reports

// Bounded hops
MATCH (n)-[:KNOWS]->{1,4}(m)
RETURN n, m

// Exactly n hops
MATCH (n)-[:KNOWS]->{3}(m)
RETURN n, m
```

### With Relationship Types
```cypher
// Single type
MATCH (n)-[:FRIEND]->+(m)
RETURN n, m

// Multiple types (OR semantics)
MATCH (n)-[:KNOWS|FRIEND]->+(m)
RETURN n, m
```

### With Properties
```cypher
// Property filter applies to ALL hops
MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m
```

### Mixed with Regular Patterns
```cypher
// Quantified followed by regular
MATCH (n)-[:KNOWS]->*(m)-->(c:Crime)
RETURN n, m, c

// Multiple quantified segments
MATCH (a)-[:FRIEND]->+(b)-[:WORKS_AT]->{1,2}(c:Company)
RETURN a, b, c

// Sandwiched patterns
MATCH (a)-[:KNOWS]->+(b)<-[:REPORTS_TO]-(c)
RETURN a, b, c
```

### Different Directions
```cypher
// Incoming
MATCH (manager)<-[:REPORTS_TO]-+(employee)
RETURN manager, employee

// Undirected
MATCH (person1)-[:CONNECTED]-+(person2)
RETURN person1, person2
```

### Empty Relationship Type (Wildcard)
```cypher
// Matches any relationship type
MATCH (n)-[]->+(m)
RETURN n, m
```

---

## Restrictions in Phase 1

### Cannot Return Relationships
```cypher
// ERROR: Cannot return relationship variable from quantified pattern
MATCH (n)-[r]->+(m)
RETURN n, r, m
```

### Cannot Use Relationship Variables
```cypher
// ERROR: Relationship variables not supported in quantified patterns
MATCH (n)-[r:KNOWS]->+(m)
RETURN n, m
```

**Why:** Relationships in quantified patterns form a list. Phase 1 doesn't have list type support yet.

### Quantifiers Must Be Literals
```cypher
// ERROR: Expected literal integer
MATCH (n)-[:KNOWS]->{1,$maxHops}(m)
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
ERROR: Cannot return relationship variables from quantified patterns
  MATCH (n)-[r]->+(m) RETURN r
              ^
HINT: Relationship variables in quantified patterns will be supported in Phase 2.
      For now, you can only return the start and end nodes.

ERROR: Relationship variables not allowed in quantified patterns
  MATCH (n)-[r:KNOWS]->+(m)
             ^
HINT: Remove the variable name: -[:KNOWS]->+

ERROR: Dynamic quantifiers not supported
  MATCH (n)-[:KNOWS]->{1,$max}(m)
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

A quantified pattern `(n)-[:REL]->{min,max}(m)` matches if there exists a path:
- Starting at node `n`
- Ending at node `m`
- Consisting of `k` relationships of type `REL` (where `min ≤ k ≤ max`)
- Following the specified direction
- All relationships matching any property constraints

### Zero-or-More Semantics

`->*` and `->{0,}` include the starting node:

```cypher
// Graph: (A)-[:MANAGES]->(B)-[:MANAGES]->(C)

MATCH (n:Person {name: 'A'})-[:MANAGES]->*(m)
RETURN m

// Results: A (0 hops), B (1 hop), C (2 hops)
```

This is useful for hierarchical queries where you want to include the root.

### Property Filter Semantics

Property filters must match **ALL** relationships in the path:

```cypher
// Graph: (A)-[:KNOWS {since: 2020}]->(B)-[:KNOWS {since: 2021}]->(C)

MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m

// Result: (A, B) only
// Does NOT return (A, C) because the second hop has since: 2021
```

---

## Key Design Decisions

### Path Uniqueness
**Status: To be filled**

**Question:** Should we return multiple rows for multiple paths between the same (n, m) pair?

**Option A - One row per path:**
```
Graph: A-[:REL]->B-[:REL]->C
       A-[:REL]->D-[:REL]->C

MATCH (a)-[:REL]->+(c) RETURN a, c

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

**Decision:** _To be filled by team_

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

MATCH (a)-[:REL]->+(b) could match:
  A->B (1 hop)
  A->B->A (2 hops)
  A->B->A->B (3 hops)
  ... potentially infinite
```

**Option B - Forbid cycles (path cannot revisit nodes):**
```
Same graph:
MATCH (a)-[:REL]->+(b)

Results:
  A->B
  B->A
  (no paths that revisit nodes)
```

**Option C - Configurable:**
- Global setting
- Query-level hint
- Separate operators for cycle-allowing vs cycle-forbidding

**Decision:** _To be filled by team_

**Implications:**
- Option A: More flexible but requires max hop limit for safety
- Option B: Simpler, safer, predictable result sizes
- Option C: Most flexible but adds complexity

---

### Empty Relationship Type Behavior

**Question:** Should `-[]->+` match any relationship type?

**Decision: Yes (wildcard)**

```cypher
// Matches any relationship type
MATCH (n)-[]->+(m)
RETURN n, m
```

This aligns with Neo4j behavior and user expectations.

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

Query: MATCH (a)-[:FRIEND]->+(c) RETURN a, c
```

One row per path: 4 rows
- (A, B)
- (A, C) via B
- (A, D)
- (A, C) via D

One row per endpoint pair: 3 rows
- (A, B)
- (A, C)
- (A, D)

---

## Parser Considerations

The grammar requires **lookahead** to distinguish quantified from regular relationships:

```cypher
-[:REL]->+     // quantified relationship
-[:REL]->(n)   // regular relationship followed by node
```

**Suggested approach:**
1. Parse relationship pattern (direction, types, properties)
2. Check next token for quantifier (`*`, `+`, `{`)
3. If quantifier present → create `QuantifiedRelationship` AST node
4. If no quantifier → create `Relationship` AST node

**Key requirement:** Relationship variables are only allowed in non-quantified relationships.

---

## AST Representation

Suggested AST structure for quantified relationships:

```
QuantifiedRelationship {
    direction: Direction        // LEFT, RIGHT, BOTH
    relationshipTypes: List<String>
    properties: PropertyMap
    quantifier: Quantifier
}

Quantifier {
    type: QuantifierType        // ZERO_OR_MORE, ONE_OR_MORE, RANGE
    min: Optional<Integer>
    max: Optional<Integer>
}

Direction {
    LEFT    // <-
    RIGHT   // ->
    BOTH    // -
}
```

**Examples:**
- `->+` → `Quantifier(ONE_OR_MORE, min=1, max=null)`
- `->*` → `Quantifier(ZERO_OR_MORE, min=0, max=null)`
- `->{3,7}` → `Quantifier(RANGE, min=3, max=7)`
- `->{5,}` → `Quantifier(RANGE, min=5, max=null)`
- `->{,10}` → `Quantifier(RANGE, min=0, max=10)`

---

## Example Test Cases

### Test 1: Basic One-or-More
```cypher
CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})
       -[:KNOWS]->(c:Person {name: 'C'})

MATCH (n)-[:KNOWS]->+(m)
RETURN n.name, m.name

Expected: (A,B), (A,C), (B,C)
```

### Test 2: Zero-or-More Includes Self
```cypher
MATCH (n:Person {name: 'A'})-[:KNOWS]->*(m)
RETURN m.name

Expected: A, B, C
```

### Test 3: Bounded Hops
```cypher
MATCH (n)-[:KNOWS]->{1,1}(m)
RETURN n.name, m.name

Expected: (A,B), (B,C)
```

### Test 4: No Path
```cypher
CREATE (isolated:Person {name: 'D'})
MATCH (n)-[:KNOWS]->+(isolated)
RETURN n.name

Expected: (empty result)
```

### Test 5: Property Filtering
```cypher
CREATE (a)-[:KNOWS {since: 2020}]->(b)-[:KNOWS {since: 2021}]->(c)

MATCH (n)-[:KNOWS {since: 2020}]->+(m)
RETURN n, m

Expected: (a,b) only
// Does NOT match a->c because second hop has since: 2021
```

### Test 6: Cycle Graph (depends on cycle handling decision)
```cypher
CREATE (a:Node)-[:REL]->(b:Node)-[:REL]->(a)

MATCH (n)-[:REL]->+(m)
RETURN n, m

Expected (if cycles forbidden): (a,b), (b,a)
Expected (if cycles allowed): (a,b), (b,a), (a,b,a), (b,a,b), ...
```

### Test 7: Mixed with Regular Patterns
```cypher
MATCH (a)-[:KNOWS]->+(b)-[:LIVES_IN]->(c:City)
RETURN a.name, c.name

Expected: All people transitively connected through KNOWS who live in cities
```

### Test 8: Multiple Quantified Patterns
```cypher
MATCH (a)-[:FRIEND]->+(b)-[:COLLEAGUE]->+(c)
RETURN a, b, c

Expected: Paths with both quantified segments satisfied
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
