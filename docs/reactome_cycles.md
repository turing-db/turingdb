# Cycles in Reactome: unbounded variable-length paths

A reaction lies on a cycle when a `precedingEvent` walk leaving it comes back to it. The
pattern writes that directly — the walk ends on the variable it started from, with no hop
bound:

```
MATCH (r:Reaction)-[:precedingEvent]->+(r) RETURN count(DISTINCT r.stId)
```

4,492 of Reactome's 83,459 reactions lie on such a cycle, in 97.60 ms.

Provenance: `2d9944925`, default `-O3` build, 20-core Linux box, v3 behind the shell's
`#v3` prefix. Graph `reactome` loaded from its binary dump in 2.9 s — the same 2,978,202
nodes and 11,537,843 edges as `path_bench.md`. Every timing below is the median of five
runs with a sixth discarded as a warmup, except where the run count is given. The box is
shared, so compare ratios within a run rather than across days.

## Unbounded cycle detection

| Query | Answer | Median |
|---|---|---|
| `(r:Reaction)-[:precedingEvent]->+(r)` | 4,492 | 97.60 ms |
| the same walked backwards, `<-[:precedingEvent]-+` | 4,492 | 97.34 |
| restricted to `{speciesName:'Homo sapiens'}` | 876 | 59.84 |
| over `(e:Event)`, so pathways as well as reactions | 4,944 | 116.38 |
| `(p:Pathway)-[:hasEvent]->+(p)` | 0 | 35.84 |
| `(c:Complex)-[:hasComponent]->+(c)` | 0 | 32.91 |

Direction does not change the set, which is the check that the end constraint is doing
what it claims. The two containment relations are acyclic: no pathway contains itself and
no complex is its own sub-unit.

## Naming the cycles

Two unbounded walks in one query — the cycle, and the pathway hierarchy that holds it:

```
MATCH (t:TopLevelPathway)-[:hasEvent]->+(r:Reaction), (r)-[:precedingEvent]->+(r)
RETURN t.displayName, count(DISTINCT r.stId)
```

24 rows in 100.09 ms, one walk of 83,459 seeds and one of 415:

| Top-level pathway | Reactions on a cycle |
|---|---|
| Metabolism | 2,873 |
| Signal Transduction | 354 |
| Transport of small molecules | 322 |
| Autophagy | 229 |
| Metabolism of proteins | 166 |
| Disease | 94 |
| Cell Cycle | 88 |
| Drug ADME | 86 |
| Drosophila signaling pathways | 72 |
| Hemostasis | 67 |
| Neuronal System | 57 |
| Muscle contraction | 56 |

The remaining 12 rows carry 46 or fewer.

Grouping by the pathway that directly holds the reaction costs the same: 271 rows in
98.88 ms across all species, 241 rows in 98.30 ms for human. The classical metabolic
cycles are all there — Citric acid cycle (TCA cycle) 106 reactions across species,
Malate-aspartate shuttle 83, Pyruvate metabolism 45, Glycine degradation 42, Urea cycle
40, Glycogen breakdown 26.

Listing the members of one costs the same again, 97.55 ms for the human TCA cycle
(`R-HSA-71403`, 10 rows) and 97.70 ms for the urea cycle (`R-HSA-70635`, 5 rows):

```
MATCH (p:Pathway {stId:'R-HSA-71403'})-[:hasEvent]->(r:Reaction), (r)-[:precedingEvent]->+(r)
RETURN DISTINCT r.stId, r.displayName

CS acetylates OA to citrate            ACO2 isomerizes citrate
IDH3 complex decarboxylates isocitrate IDH2 dimer decarboxylates isocitrate
SUCLG1/A2 cleaves succinyl-CoA         SUCLG1/G2 cleaves succinyl-CoA
SDH complex dehydrogenates succinate   FH tetramer hydrates fumarate to L-malate
MDH2 dimer dehydrogenates MAL          NNT dimer transfers proton from NADPH to NAD+
```

## What the bound would have cost

The same query with a hop bound, to see where the unbounded answer comes from:

| Bound | Reactions on a cycle | Median |
|---|---|---|
| `{1,2}` | 2,240 | 9.46 ms |
| `{1,3}` | 3,039 | 12.51 |
| `{1,4}` | 3,568 | 15.52 |
| `{1,6}` | 4,045 | 21.30 |
| `{1,8}` | 4,217 | 26.34 |
| `{1,12}` | 4,319 | 36.00 |
| `{1,16}` | 4,398 | 43.82 |
| `{1,24}` | 4,464 | 59.33 |
| `{1,40}` | 4,492 | 85.73 |
| `+` | 4,492 | 97.60 |

Half the cycles close in 2 hops. 94 reactions close only past 16 hops and 28 only past 24,
so a bound a curator would think to write misses them. The unbounded form costs 14 % more
than the bound that first reaches the full answer, and it does not need to know the
number 40 in advance.

## What does not finish

All of the above binds no path. Binding one — `-[e:precedingEvent]->+(r)` — asks for the
closed trails themselves, and there are too many. From the single seed
`R-HSA-70975` (CS acetylates OA to citrate):

| Bound | Closed trails | Median |
|---|---|---|
| `{1,10}` | 46 | 1.69 ms |
| `{1,16}` | 1,546 | 15.26 |
| `{1,20}` | 13,576 | 120.04 |
| `{1,24}` | 98,382 | 869.07 (n=3) |
| `{1,28}` | 570,310 | 5,152.75 (n=2) |
| `{1,32}` | 2,481,686 | 26,578.90 (n=2) |

About 6.5× per 4 hops. Unbounded, three shapes were killed rather than answered:

| Query | Killed at |
|---|---|
| `(r:Reaction {stId:'R-HSA-70975'})-[e:precedingEvent]->+(r) RETURN count(e)` | 240 s |
| the 10 TCA seeds, `RETURN count(e)` | 600 s |
| the 10 TCA seeds, no path bound but `RETURN r.stId, r.displayName` | 600 s |

The third one is the trap. It asks the same question as the 97 ms query above and differs
only in that it returns rows instead of a set, so `fuse_explore_distinct_ends` leaves the
exploration alone and the walk enumerates trails. Writing `RETURN DISTINCT r.stId,
r.displayName` gives the 10 rows in 97.55 ms.

Untyped is the other cliff. Walking every edge type instead of `precedingEvent` finds more
cycles, over a fan-out that is ~30× larger:

```
MATCH (r:Reaction)-[x]->+(r) RETURN count(DISTINCT r.stId)
6,368     537,726 ms (one run)
```
