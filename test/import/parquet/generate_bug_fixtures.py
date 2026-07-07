#!/usr/bin/env python3
"""Generates the Parquet fixtures used by ParquetImporterTest's bug-demonstration
tests (findings 1-5 from the branch code review).

Each fixture is a nodes/edges pair in the split-Parquet import format:
  nodes: __id INT64, __labels LIST<BYTE_ARRAY>, plus any property columns
  edges: __source INT64, __target INT64, __type BYTE_ARRAY, plus properties

The node/edge column layout mirrors the existing nodes.parquet / edges.parquet
fixtures (pyarrow's default LIST encoding yields the __labels.list.element path
the importer expects). Run from this directory:

    python3 generate_bug_fixtures.py
"""

import pyarrow as pa
import pyarrow.parquet as pq

LABELS_TYPE = pa.list_(pa.binary())


def write(table, path, **kwargs):
    pq.write_table(table, path, compression="none", **kwargs)
    print(f"wrote {path} ({table.num_rows} rows)")


def minimal_edges():
    # A single valid edge (0 -> 1). Node-focused fixtures all define nodes 0 and 1,
    # so this imports cleanly once the node bug under test is fixed.
    table = pa.table(
        {
            "__source": pa.array([0], pa.int64()),
            "__target": pa.array([1], pa.int64()),
            "__type": pa.array([b"LINKS"], pa.binary()),
        }
    )
    write(table, "minimal_edges.parquet")


def multipage_string_nodes():
    # Finding 1: a BYTE_ARRAY (string) property column large enough to split across
    # many data pages within one row group. A tiny page size + no dictionary forces
    # the split so capturePropertyByteArray's per-page overwrite drops all but the
    # last page's values.
    count = 500
    ids = list(range(count))
    labels = [[b"Person"] for _ in range(count)]
    names = [f"person_number_{i:05d}_padded_so_the_value_is_wide_enough" for i in range(count)]
    table = pa.table(
        {
            "__id": pa.array(ids, pa.int64()),
            "__labels": pa.array(labels, LABELS_TYPE),
            "name": pa.array(names, pa.binary()),
        }
    )
    write(
        table,
        "multipage_string_nodes.parquet",
        use_dictionary=False,
        data_page_size=100,
        write_batch_size=16,
    )

if __name__ == "__main__":
    minimal_edges()
    multipage_string_nodes()
    empty_labels_nodes()
    nulltype_nodes_and_edges()
    list_property_nodes()
    wrongtype_id_nodes()
