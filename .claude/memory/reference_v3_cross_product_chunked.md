---
name: reference-v3-cross-product-chunked
description: v3's nl.cross_product is an iterator that drives its own nl.for, emitting one chunk of pairs per step; both engines hold one chunk whatever the sides measure
metadata:
  type: reference
---

The cross product is the one operator whose result outgrows its inputs, so in v3 it is
an **iterator**, not a straight-line op: `nl.cross_product {outer} {inner}` returns
`!nl.iter<...>` and `DBLowering::lowerCrossProduct` drives it with `buildLoopForSource`,
exactly as a scan is driven. That loop is a third level below the two the factors open,
and it is where the consumer (the lowered `db.output`) goes:

    nl.for %a in %scanA { nl.for %b in %scanB {
      %pairs = nl.cross_product {%a} {%b}
      nl.for %ra, %rb in %pairs { nl.output(%ra, %rb) }
    } }

`NLExecutor::runCrossProductLoop` walks the N*M pairs with a single `position` cursor,
emitting `min(chunkSize, remaining pairs)` per step - the v2 `CartesianProductProcessor`
design, whose cursor is `_lhsPtr`/`_rhsPtr`. The broadcasts take that slice: pair p is
outer row `p / M` (`blockRepeatColumn`) with inner row `p % M` (`tileColumn`), so a step
may start and end mid-block or mid-tile. `factor` is M for **both** directions.

Consequences worth knowing:
- **Peak memory is one chunk per product**, not the product. A three-way MATCH nests two
  products, each with its own loop, so it is O(depth * chunkSize).
- A `LIMIT` bounds it through the ordinary loop early-exit (`nl.for ... limit %h`), like
  a scan's; the op carries no limit operand of its own.
- Every column of a side must be row-aligned with that side's first column - that is what
  lets one position slice them all.

Before 2026-09-09 the op was straight-line and laid out the whole chunk-against-chunk
product in one step, so two 64Ki-row sides meant 4.29e9 rows - 64 GiB for two id columns,
and reliable OOM kills. Measured after the change on reactome: `MATCH (a:Reaction),
(b:Reaction) RETURN a, b` (6.97e9 rows) runs in 1.76 s holding 1.9 MiB, against 3.8 s /
8.3 GiB for the 542M-row `Pathway x Pathway` before it. `samples/cartesian_bench/`
benchmarks v2 against v3 and guards the product size; `test/query/ir/
CrossProductChunkedTest.cpp` pins that no chunk exceeds the chunk size.
