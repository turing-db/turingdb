# Overview: Cypher Quantified Patterns

## What Cypher Supports

Neo4j's Cypher query language has **two overlapping syntaxes** for variable-length path matching:

### 1. Variable-Length Patterns (Legacy)
Uses `*` inside the relationship brackets:
```cypher
(a)-[*]->(b)           // 0 or more hops
(a)-[*1..]->(b)        // 1 or more hops
(a)-[*0..5]->(b)       // 0 to 5 hops
(a)-[*3..7]->(b)       // 3 to 7 hops
(a)-[r*..5 {name: 'X'}]->(b)  // with properties
```

**Problems with this syntax:**
- The `*` character is overloaded (both a marker and a wildcard)
- Ambiguous parsing (is `*` the lower bound or just a marker?)
- Context-sensitive tokenization
- Implicit zero bounds
- Difficult to extend

### 2. Quantified Relationships (Modern)
Uses quantifiers `*`, `+`, `{n,m}` **after** the relationship brackets:
```cypher
(a)-[]->*              // 0 or more hops
(a)-[]->+              // 1 or more hops
(a)-[]->{1,5}          // 1 to 5 hops
(a)-[]->{3,7}          // 3 to 7 hops
(a)-[:REL {prop: 'X'}]->+    // with types and properties
```

**Advantages:**
- Clear, unambiguous syntax
- Regex-like quantifiers (familiar to users)
- Easier to parse and extend
- Better composition with other pattern features

### 3. Parenthesized Path Patterns
Complex patterns with repetition:
```cypher
(start) ((n:Stop)-[:NEXT]->(m:Stop)){1,10} (end)
```

---

## Our Implementation Approach

**We will implement ONLY the modern quantified relationship syntax.**

### What We're Implementing

✅ **Quantified relationships** with all quantifiers:
- `->+` (one or more)
- `->*` (zero or more)
- `->{n,m}` (bounded range)
- `->{n,}` (at least n)
- `->{,m}` (at most m)
- `->{n}` (exactly n)

✅ **All edge directions:**
- `->` (outgoing)
- `<-` (incoming)
- `-` (undirected)

✅ **Relationship type and property filtering:**
```cypher
-[:KNOWS|FRIEND]->+
-[:KNOWS {since: 2020}]->*
```

✅ **Complex queries with mixed patterns:**
```cypher
MATCH (a)-[:FRIEND]->+(b)-[:WORKS_AT]->{1,2}(c:Company)
RETURN a, b, c
```

### What We're NOT Implementing

❌ **Legacy variable-length syntax:**
```cypher
-[*]->              // NOT supported
-[*1..5]->          // NOT supported
-[r*..5 {p: 'v'}]-> // NOT supported
```

**Rationale:** The legacy syntax is ambiguous, difficult to parse, and adds significant complexity. Neo4j maintains it only for backward compatibility. Since we're building from scratch, we can skip this technical debt.

If users need to migrate queries from Neo4j, we'll provide a migration guide showing the translation:
```
[*1..]  →  ->+
[*]     →  ->*
[*0..5] →  ->{0,5}
```

---

## Phase Breakdown

### [Phase 1: Basic Quantified Path Patterns](phase-1-spec.md)

**Scope:** Basic quantified relationships without intermediate variable returns.

**You can write:**
```cypher
MATCH (n)-[:KNOWS]->+(m) RETURN n, m
MATCH (a)-[:REL]->{1,5}(b)-->(c) RETURN a, b, c
```

**You cannot:**
- Return relationships: `MATCH (n)-[r]->+(m) RETURN r`
- Use relationship variables: `MATCH (n)-[r:KNOWS]->+(m)`
- Use parenthesized patterns: `((a)-[:REL]->(b)){1,5}`
- Use WHERE clauses in patterns
- Use expression-based quantifiers: `->{1,$maxHops}`

---

### [Phase 2: Relationship Return Support](phase-2-spec.md)

**Scope:** Add ability to return relationships from quantified patterns.

**Requires:**
- List/array type support in the type system
- Relationships returned as lists

**New capabilities:**
```cypher
MATCH (n)-[r]->+(m) RETURN n, r, m
-- r is a list: [rel1, rel2, ..., relN]
```

---

### [Phase 3: Parenthesized Quantified Patterns](phase-3-spec.md)

**Scope:** Support `((pattern)){n,m}` with full variable binding.

**New capabilities:**
```cypher
MATCH (start) ((n:Stop)-[:NEXT]->(m:Stop)){1,10} (end)
RETURN n, m
-- n and m are lists of intermediate nodes
```

---

### [Phase 4: Path Pattern WHERE Clauses](phase-4-spec.md)

**Scope:** Support filtering within quantified patterns.

**New capabilities:**
```cypher
MATCH (n)-[:KNOWS]->+ WHERE m.age > 18 (m)
RETURN n, m
```

---

## Neo4j Compatibility Matrix

| Feature | Neo4j | Our Implementation |
|---------|-------|-------------------|
| Quantified relationships (`->+`, `->*`, `->{n,m}`) | ✅ | ✅ Phase 1 |
| Variable-length patterns (`-[*]->`, `-[*1..5]->`) | ✅ | ❌ Not supported |
| Relationship type filters | ✅ | ✅ Phase 1 |
| Property filters | ✅ | ✅ Phase 1 |
| Return relationships from quantified patterns | ✅ | ✅ Phase 2 |
| Parenthesized path patterns | ✅ | ✅ Phase 3 |
| WHERE clauses in patterns | ✅ | ✅ Phase 4 |
| Path variables | ✅ | ❌ TBD |
| Dynamic quantifiers (expressions in `{...}`) | ❌ | ❌ TBD |

---

## Why This Phased Approach?

### Phase 1: Minimal Viable Implementation
- Gets core path matching working
- No complex type system changes needed
- Users can run most common queries
- Builds foundation for later phases

### Phase 2: Type System Extension
- Adds list support (needed for multiple relationships)
- Enables more sophisticated queries
- Maintains backward compatibility with Phase 1

### Phase 3: Advanced Patterns
- Enables complex graph traversals
- Supports Neo4j's advanced pattern syntax
- Useful for sophisticated applications

### Phase 4: Filtering Flexibility
- Adds expressive filtering capabilities
- Reduces need for post-processing in application code
- Performance optimization opportunities

---

## Design Principles

1. **Simplicity First:** Start with the cleanest syntax possible
2. **No Legacy Baggage:** Don't inherit Neo4j's historical compromises
3. **Clear Semantics:** Every feature should have unambiguous meaning
4. **Performance Aware:** Design for columnar batch processing from the start
5. **User-Friendly Errors:** Guide users toward correct syntax

---

## Next Steps

1. Review [Phase 1 Specification](phase-1-spec.md)
2. Make key design decisions (path uniqueness, cycle handling, max hops)
3. Review [Implementation Notes](implementation-notes.md) for algorithmic approach
4. Set up [Testing Strategy](testing-strategy.md)
5. Begin implementation
