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

import datetime

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

def nested_list_property_nodes():
    # A LIST<LIST<INT64>> property column: two repetition levels, so the importer has to
    # reassemble the inner lists from the rep/def levels rather than flatten them. Covers
    # an empty inner list, a null inner list, a null element and an empty outer list.
    ids = [0, 1, 2, 3]
    labels = [[b"Person"] for _ in ids]
    groups = [
        [[1, 2], [3]],
        [[], [4, None]],
        [None, [5]],
        [],
    ]
    table = pa.table(
        {
            "__id": pa.array(ids, pa.int64()),
            "__labels": pa.array(labels, LABELS_TYPE),
            "groups": pa.array(groups, pa.list_(pa.list_(pa.int64()))),
        }
    )
    write(table, "nested_list_property_nodes.parquet")


def datetime_property_nodes():
    # A TIMESTAMP column in each unit Parquet counts in, plus a tz-naive one and a list of
    # timestamps. The importer reads the unit off the logical type and scales every column
    # to the microseconds a DateTime holds, so all four scalar columns name one instant:
    # 2024-03-14T09:30:00Z. The nanosecond column also carries a sub-microsecond digit on
    # one row, which floors, and one row is null in every column.
    instant = datetime.datetime(2024, 3, 14, 9, 30, 0, tzinfo=datetime.timezone.utc)
    earlier = datetime.datetime(1969, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)

    ids = [0, 1, 2]
    labels = [[b"Event"], [b"Event"], [b"Event"]]

    # Written through int64 arrays cast to each unit, so the exact on-disk counts are
    # pinned here rather than left to a datetime object's own resolution
    micros = [1710408600000000, -1000000, None]
    millis = [1710408600000, -1000, None]
    nanos = [1710408600000000000, -1000000000 - 1, None]

    table = pa.table(
        {
            "__id": pa.array(ids, pa.int64()),
            "__labels": pa.array(labels, LABELS_TYPE),
            "atMicros": pa.array(micros, pa.int64()).cast(pa.timestamp("us", tz="UTC")),
            "atMillis": pa.array(millis, pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
            "atNanos": pa.array(nanos, pa.int64()).cast(pa.timestamp("ns", tz="UTC")),
            # No tz: Parquet records isAdjustedToUTC=false, and the wall clock is read as UTC
            "atNaive": pa.array(micros, pa.int64()).cast(pa.timestamp("us")),
            "atList": pa.array(
                [[instant, earlier], [], None],
                pa.list_(pa.timestamp("us", tz="UTC")),
            ),
        }
    )
    write(table, "datetime_property_nodes.parquet")


def datetime_property_edges():
    # One edge carrying a timestamp, so the edge visitor's own switch is exercised too
    table = pa.table(
        {
            "__source": pa.array([0], pa.int64()),
            "__target": pa.array([1], pa.int64()),
            "__type": pa.array([b"HAPPENED"], pa.binary()),
            "at": pa.array([1710408600000000], pa.int64()).cast(pa.timestamp("us", tz="UTC")),
        }
    )
    write(table, "datetime_property_edges.parquet")


def datetime_out_of_range_nodes():
    # A TIMESTAMP(MILLIS) whose instant falls outside the year range an ISO-8601 string
    # with a four-digit year can spell. 1e15 ms is year 33658: it clears the millis
    # overflow guard, so only a range check on the instant itself turns it away.
    # Two nodes, so minimal_edges.parquet's 0 -> 1 resolves and the import reaches the
    # property it is really about
    table = pa.table(
        {
            "__id": pa.array([0, 1], pa.int64()),
            "__labels": pa.array([[b"Event"], [b"Event"]], LABELS_TYPE),
            "at": pa.array([10**15, 0], pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
        }
    )
    write(table, "datetime_out_of_range_nodes.parquet")


def datetime_name_clash_fixtures():
    # One property name discovered at two types across a split export: TIMESTAMP on the
    # nodes, plain INT64 on the edges. A DateTime and an Int64 are the same eight bytes, so
    # without a check the edge's 42 reads back as 1970-01-01T00:00:00.000042Z.
    nodes = pa.table(
        {
            "__id": pa.array([0, 1], pa.int64()),
            "__labels": pa.array([[b"Event"], [b"Event"]], LABELS_TYPE),
            "ts": pa.array([1710408600000000, 0], pa.int64()).cast(pa.timestamp("us", tz="UTC")),
        }
    )
    write(nodes, "datetime_name_clash_nodes.parquet")

    edges = pa.table(
        {
            "__source": pa.array([0], pa.int64()),
            "__target": pa.array([1], pa.int64()),
            "__type": pa.array([b"LINKS"], pa.binary()),
            "ts": pa.array([42], pa.int64()),
        }
    )
    write(edges, "datetime_name_clash_edges.parquet")


if __name__ == "__main__":
    minimal_edges()
    multipage_string_nodes()
    nested_list_property_nodes()
    datetime_property_nodes()
    datetime_property_edges()
    datetime_out_of_range_nodes()
    datetime_name_clash_fixtures()
