# OptimusKG sample

Download the [OptimusKG](https://optimuskg.ai/) biomedical knowledge graph
parquet files from Harvard Dataverse and load them into TuringDB using the
`turing-parquet` tool.

OptimusKG has 190,531 nodes across 10 entity types (Gene, Disease, Drug,
Phenotype, Anatomy, …) and ~21.8M edges across 26 relation types.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/) for managing the Python environment.
- A built turingdb with `turing-parquet` on `PATH` (run `source setup.sh`
  from the repo root after `make install`).

## Run

```bash
./download.sh   # fetches data/nodes.parquet (~156 MB) and data/edges.parquet (~170 MB)
./import.sh     # writes ./turingdb.out/ containing the `optimuskg` graph
```

The cached parquet files and the generated graph directory are git-ignored.

## Query the graph

After import:

```bash
turingdb start -demon -turing-dir ./turingdb.out
python3 - <<'PY'
from turingdb import TuringDB
c = TuringDB(host="http://localhost:6666")
c.load_graph("optimuskg")
c.set_graph("optimuskg")
print(c.query("MATCH (n:GEN) WHERE n.symbol = 'TSPAN6' RETURN n.id, n.symbol, n.biotype"))
PY
turingdb stop -turing-dir ./turingdb.out
```
