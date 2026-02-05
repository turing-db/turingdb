# Phase 4: Path Pattern WHERE Clauses

**Status:** To be filled  
**Prerequisites:** Phase 1, 2, and 3 complete

---

## Overview

Phase 4 adds support for WHERE clauses within path patterns, allowing inline filtering during traversal for better performance and expressiveness.

---

## Planned Features

**WHERE clauses in quantified patterns:**
```cypher
MATCH (n)-[:KNOWS]->+ WHERE m.age > 18 (m)
RETURN n, m
```

**Complex filters:**
```cypher
MATCH (a) ((n)-[:REL]->(m)){1,5} WHERE n.type = 'important' (b)
RETURN a, b
```

---

## Requirements

- [ ] WHERE clause parsing in patterns
- [ ] Filter evaluation during traversal
- [ ] Optimization opportunities
- [ ] Variable scope handling

---

## Open Questions

1. When should filters be evaluated (eager vs lazy)?
2. Can filters reference variables from outer patterns?
3. What functions/operators are allowed in WHERE?
4. Should we support EXISTS subqueries in WHERE?

---

_This document will be completed after Phase 3 implementation._
