# OptimusKG — graph summary

Biomedical knowledge graph integrating 65 resources grounded in 18 ontologies
(BioCypher framework, Biolink Model).
DOI: [10.7910/DVN/IYNGEV](https://doi.org/10.7910/DVN/IYNGEV).
Site: <https://optimuskg.ai/>.

## Source files

- `nodes.parquet` — **190,531 rows × 3 columns** (`id`, `label`, `properties`)
- `edges.parquet` — **21,813,816 rows × 6 columns** (`from`, `to`, `label`, `relation`, `undirected`, `properties`)

## TuringDB graph (after JSON sub-record expansion)

- **5,022,579 nodes** = 190,531 entities + 4,832,048 sub-records
- **30,262,127 edges** = 21,813,816 relations + 8,448,311 `HAS_*` sub-record edges
- 62 distinct labels, 88 distinct edge types, 129 distinct property types

3,616,263 sub-record observations dedup into shared nodes
(`id`/`value` natural keys with full-content fallback) — 42.8% dedup ratio.

## Entity labels (10 root types, 190,531 nodes)

| Label | Count | Meaning |     | Label | Count | Meaning |
|---|---:|---|---|---|---:|---|
| GEN | 61,306 | Gene |  | ANA | 13,120 | Anatomy |
| DIS | 36,345 | Disease |  | MFN | 10,161 | Molecular function |
| BPO | 25,754 | Biological process |  | CCO | 4,052 | Cellular component |
| PHE | 19,341 | Phenotype |  | PWY | 2,805 | Pathway |
| DRG | 16,766 | Drug |  | EXP | 881 | Experiment |

## Cross-entity edge types (top 8)

| Edge type | Count |
|---|---:|
| `ASSOCIATED_WITH` | 10,531,730 |
| `EXPRESSION_PRESENT` | 6,616,463 |
| `EXPRESSION_ABSENT` | 2,171,492 |
| `SYNERGISTIC_INTERACTION` | 1,341,086 |
| `INTERACTS_WITH` | 734,862 |
| `PHENOTYPE_PRESENT` | 157,144 |
| `PARENT` | 95,711 |
| `IS_A` | 61,720 |

## Sub-record types (`HAS_*` edges, top 8 of 27)

| Edge type | Edges | Target label | Distinct nodes |
|---|---:|---|---:|
| `HAS_HOMOLOGUES` | 3,819,222 | `Homologue` | 2,002,566 |
| `HAS_CANONICAL_EXONS` | 585,060 | `CanonicalExon` | 570,354 |
| `HAS_SYNONYMS` | 577,855 | `Synonym` | 559,240 |
| `HAS_XREFS` | 529,206 | `Xref` | 272,922 |
| `HAS_TRACTABILITY` | 528,500 | `Tractability` | **19** |
| `HAS_TRANSCRIPT_IDS` | 324,631 | `TranscriptId` | 324,631 |
| `HAS_ASSOCIATED_PROTEINS` | 266,911 | `AssociatedProtein` | 265,647 |
| `HAS_SOURCES` | 190,531 | `Source` | **16** |

## Heavily-deduplicated sub-records

| Label | Distinct nodes | Inbound refs | Reuse |
|---|---:|---:|---:|
| `Tractability` | 19 | 528,500 | 27,815× |
| `Origin` | 2 | 4,783 | 2,392× |
| `Url` | 17 | 5,865 | 345× |
| `SubcellularLocation` | 906 | 56,879 | 63× |
| `TargetClass` | 565 | 14,238 | 25× |

## Property keys

129 globally-registered property types. Most are `String`; sub-records mix in
`Int64` / `Double` / `Bool`. Property-name collisions across sub-records
(`strand` is `String` in `:CanonicalTranscript` but `Int64` in `:GenomicLocation`)
are auto-mangled with a value-type tag — so the catalog carries both
`strand` and `strand (Int64)`, and queries pick the one matching the label.

## Reproducing this

From this directory:

```bash
./download.sh         # ~326 MB into ./data/, via the optimuskg Python client
./import.sh           # ~2m20s, writes ./turingdb.out/ with graph name "optimuskg"
```
