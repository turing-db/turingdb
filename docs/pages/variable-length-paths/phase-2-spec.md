# Phase 2: Relationship Return Support

**Status:** To be filled  
**Prerequisites:** Phase 1 complete, List type support in type system

---

## Overview

Phase 2 adds the ability to return relationships from quantified patterns. This requires implementing list/array types in the type system, as relationships in quantified patterns form collections.

---

## Planned Features

**Relationship variables in quantified patterns:**
```cypher
MATCH (n)-[r]->+(m)
RETURN n, r, m
// r will be a list of relationships
```

**Variable binding:**
```cypher
MATCH (n)-[rels:KNOWS]->+(m)
RETURN n, rels, m
// rels is a list of :KNOWS relationships
```

---

## Requirements

- [ ] List type implementation in type system
- [ ] Relationship list semantics
- [ ] Variable binding in quantified relationships
- [ ] Return list projection

---

## Open Questions

1. Should the list include relationships in order of traversal?
2. How to handle empty lists (zero hops with `*`)?
3. Should we support list operations (indexing, slicing)?
4. What is the size limit for relationship lists?

---

_This document will be completed after Phase 1 implementation._
