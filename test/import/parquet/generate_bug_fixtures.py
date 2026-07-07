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


def empty_labels_nodes():
    # Finding 2: a node whose __labels list is empty. fillLabels skips the empty
    # entry before emplacing the per-node label vector, misaligning labels with ids.
    ids = [0, 1, 2]
    labels = [[b"Person"], [], [b"Company"]]
    table = pa.table(
        {
            "__id": pa.array(ids, pa.int64()),
            "__labels": pa.array(labels, LABELS_TYPE),
        }
    )
    write(table, "empty_labels_nodes.parquet")


def nulltype_nodes_and_edges():
    # Finding 3: an edge whose __type is null. fillEdgeTypes never consults the
    # __type definition levels, so the delivered type count falls short of the row
    # count and the type-to-edge mapping is off.
    node_table = pa.table(
        {
            "__id": pa.array([0, 1, 2], pa.int64()),
            "__labels": pa.array([[b"N"], [b"N"], [b"N"]], LABELS_TYPE),
        }
    )
    write(node_table, "nulltype_nodes.parquet")

    edge_table = pa.table(
        {
            "__source": pa.array([0, 1, 2], pa.int64()),
            "__target": pa.array([1, 2, 0], pa.int64()),
            "__type": pa.array([b"KNOWS", None, b"WORKS_FOR"], pa.binary()),
        }
    )
    write(edge_table, "nulltype_edges.parquet")


def list_property_nodes():
    # Finding 4: a repeated (LIST) property column. discoverPropertyColumn ignores
    # repetition and registers it as a scalar property; the level/value bookkeeping
    # then goes inconsistent at chunk end.
    ids = [0, 1, 2]
    labels = [[b"Person"], [b"Person"], [b"Company"]]
    tags = [[1, 2], [3], [4, 5, 6]]
    table = pa.table(
        {
            "__id": pa.array(ids, pa.int64()),
            "__labels": pa.array(labels, LABELS_TYPE),
            "tags": pa.array(tags, pa.list_(pa.int64())),
        }
    )
    write(table, "list_property_nodes.parquet")


def wrongtype_id_nodes():
    # Finding 5: the required __id column has the wrong physical type (INT32 instead
    # of INT64). onFileStart trips a bioassert instead of raising a catchable error
    # like the missing-column branch does.
    ids = [0, 1, 2]
    labels = [[b"Person"], [b"Person"], [b"Company"]]
    table = pa.table(
        {
            "__id": pa.array(ids, pa.int32()),
            "__labels": pa.array(labels, LABELS_TYPE),
        }
    )
    write(table, "wrongtype_id_nodes.parquet")


if __name__ == "__main__":
    minimal_edges()
    multipage_string_nodes()
    empty_labels_nodes()
    nulltype_nodes_and_edges()
    list_property_nodes()
    wrongtype_id_nodes()
