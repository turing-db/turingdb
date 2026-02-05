# Phase 3: Parenthesized Quantified Patterns

**Status:** To be filled  
**Prerequisites:** Phase 1 and 2 complete

---

## Overview

Phase 3 adds support for parenthesized path patterns with quantifiers, allowing more complex path expressions with intermediate variable bindings.

---

## Planned Features

**Parenthesized patterns:**
```cypher
MATCH (start) ((n:Stop)-[:NEXT]->(m:Stop)){1,10} (end)
RETURN n, m
-- n and m are lists of intermediate nodes
```

**Nested patterns:**
```cypher
MATCH ((a)-[:REL]->(b)-[:REL]->(c)){2,5}
RETURN a, b, c
```

---

## Requirements

- [ ] Parenthesized pattern parsing
- [ ] Variable binding in subpatterns
- [ ] List semantics for intermediate nodes
- [ ] Connection to surrounding nodes

---

## Open Questions

1. Should we require explicit connectors to surrounding nodes?
2. How to handle implicit vs explicit connections?
3. What are the performance implications?
4. Should we allow nesting of parenthesized patterns?

---

_This document will be completed after Phase 2 implementation._
