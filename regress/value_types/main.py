import datetime
import math
import os
import sys

import numpy as np
import pandas as pd
import turingdb

UTC = datetime.timezone.utc

GRAPH = (
    "CREATE (:T {name: 'a', i: 1, d: 2.5, s: 'x', b: true, dt: datetime('2024-01-02T03:04:05Z'), "
    "du: duration({minutes: 5}), e: (0.5, 0.25), l: [1, 2], m: {k: 1, s: 'v', b: true}, p: point({x: 1.5, y: -2})})"
    "-[:R]->(:T {name: 'b'})"
)

PROPERTY_VALUE_TYPES = {
    "name": "String", "i": "Int64", "d": "Double", "s": "String", "b": "Bool",
    "dt": "DateTime", "du": "Duration", "e": "Embedding", "l": "List", "m": "Map", "p": "Point",
}


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="localhost")
    native.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("embedded", embedded)]


def plain(value):
    if isinstance(value, list):
        return [plain(element) for element in value]
    elif isinstance(value, dict):
        return {key: plain(element) for key, element in value.items()}
    elif isinstance(value, np.ndarray):
        return plain(value.tolist())
    elif isinstance(value, np.generic):
        return plain(value.item())
    elif value is None or value is pd.NA or value is pd.NaT:
        return None
    elif isinstance(value, float) and math.isnan(value):
        return None
    elif isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    elif isinstance(value, pd.Timedelta):
        return value.to_pytimedelta()

    return value


def entity_then_its_id(rows: list) -> None:
    assert rows[0][0] is not None and rows[0][0] == rows[0][1], rows
    assert rows[1] == [None, None], rows


def path_of_ids(rows: list) -> None:
    assert len(rows) == 1 and rows[0][0] == rows[0][1:], rows


def one_named(name: str):
    def check(rows: list) -> None:
        assert len(rows) == 1 and isinstance(rows[0][0], int) and rows[0][1] == name, rows

    return check


def property_types(rows: list) -> None:
    assert all(isinstance(row[0], int) for row in rows), rows
    assert {row[1]: row[2] for row in rows} == PROPERTY_VALUE_TYPES, rows


def cases(change: int) -> list[dict]:
    created = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    return [
        {"name": "an Int64 column",
         "query": "MATCH (n:T) RETURN COUNT { (n)-->() } + 1 AS degree ORDER BY n.name",
         "dtypes": ["Int64"], "rows": [[2], [1]]},
        {"name": "a nullable Int64 column",
         "query": "MATCH (n:T) RETURN n.i AS i ORDER BY n.name",
         "dtypes": ["Int64"], "rows": [[1], [None]]},
        {"name": "an Int64 constant",
         "query": "RETURN 1 AS one",
         "dtypes": ["Int64"], "rows": [[1]]},
        {"name": "a nullable Int64 constant",
         "query": "RETURN toInteger('42') AS parsed, toInteger('x') AS unparsed",
         "dtypes": ["Int64", "Int64"], "rows": [[42, None]]},

        {"name": "a UInt64 column",
         "query": "MATCH (n:T) RETURN count(n) AS count",
         "dtypes": ["UInt64"], "rows": [[2]]},
        {"name": "a nullable UInt64 column",
         "query": "MATCH (n:T) RETURN max(COUNT { (n)-->() }) AS most",
         "dtypes": ["UInt64"], "rows": [[1]]},
        {"name": "a nullable UInt64 column holding null",
         "query": "MATCH (n:Nope) RETURN max(COUNT { (n)-->() }) AS most",
         "dtypes": ["UInt64"], "rows": [[None]]},

        {"name": "a nullable Double column",
         "query": "MATCH (n:T) RETURN n.d AS d ORDER BY n.name",
         "dtypes": ["Double"], "rows": [[2.5], [None]]},
        {"name": "a Double constant",
         "query": "RETURN 2.5 AS half",
         "dtypes": ["Double"], "rows": [[2.5]]},
        {"name": "a nullable Double constant",
         "query": "RETURN toFloat('2.5') AS parsed, toFloat('x') AS unparsed",
         "dtypes": ["Double", "Double"], "rows": [[2.5, None]]},

        {"name": "a nullable String column",
         "query": "MATCH (n:T) RETURN n.s AS s ORDER BY n.name",
         "dtypes": ["String"], "rows": [["x"], [None]]},
        {"name": "a String constant",
         "query": "RETURN 'a' AS text",
         "dtypes": ["String"], "rows": [["a"]]},
        {"name": "a nullable String constant",
         "query": "RETURN toString(toInteger('4')) AS parsed, toString(toInteger('x')) AS unparsed",
         "dtypes": ["String", "String"], "rows": [["4", None]]},

        {"name": "a Bool column",
         "query": "MATCH (n:T) RETURN n.i IS NULL AS missing ORDER BY n.name",
         "dtypes": ["Bool"], "rows": [[False], [True]]},
        {"name": "a nullable Bool column",
         "query": "MATCH (n:T) RETURN n.b AS b ORDER BY n.name",
         "dtypes": ["Bool"], "rows": [[True], [None]]},
        {"name": "a Bool constant",
         "query": "RETURN true AS yes",
         "dtypes": ["Bool"], "rows": [[True]]},
        {"name": "a nullable Bool constant",
         "query": "RETURN toBoolean('true') AS parsed, toBoolean('x') AS unparsed",
         "dtypes": ["Bool", "Bool"], "rows": [[True, None]]},

        {"name": "a nullable Embedding column",
         "query": "MATCH (n:T) RETURN n.e AS e ORDER BY n.name",
         "dtypes": ["Embedding"], "rows": [[[0.5, 0.25]], [None]]},
        {"name": "an Embedding constant",
         "query": "RETURN (0.5, 0.25) AS e",
         "dtypes": ["Embedding"], "rows": [[[0.5, 0.25]]]},

        {"name": "a nullable DateTime column",
         "query": "MATCH (n:T) RETURN n.dt AS dt ORDER BY n.name",
         "dtypes": ["DateTime"], "rows": [[created], [None]]},
        {"name": "a nullable DateTime constant",
         "query": "RETURN datetime('2024-01-02T03:04:05Z') AS parsed, datetime(toString(toInteger('x'))) AS unparsed",
         "dtypes": ["DateTime", "DateTime"], "rows": [[created, None]]},

        {"name": "a nullable Duration column",
         "query": "MATCH (n:T) RETURN n.du AS du ORDER BY n.name",
         "dtypes": ["Duration"], "rows": [[datetime.timedelta(minutes=5)], [None]]},
        {"name": "a nullable Duration constant",
         "query": "RETURN duration(5) AS five, duration(toInteger('x')) AS unparsed",
         "dtypes": ["Duration", "Duration"], "rows": [[datetime.timedelta(microseconds=5), None]]},

        {"name": "a nullable Point column",
         "query": "MATCH (n:T) RETURN n.p AS p ORDER BY n.name",
         "dtypes": ["Point"], "rows": [[turingdb.Point(7203, 1.5, -2.0)], [None]]},

        {"name": "a null constant",
         "query": "RETURN null AS nothing",
         "dtypes": ["Null"], "rows": [[None]]},

        {"name": "a nullable node column",
         "query": "MATCH (n:T) OPTIONAL MATCH (n)-->(m) RETURN m, id(m) AS id ORDER BY n.name",
         "dtypes": ["UInt64", "Int64"], "check": entity_then_its_id},
        {"name": "a nullable edge column",
         "query": "MATCH (n:T) OPTIONAL MATCH (n)-[e]->() RETURN e, id(e) AS id ORDER BY n.name",
         "dtypes": ["UInt64", "Int64"], "check": entity_then_its_id},
        {"name": "a path column",
         "query": "MATCH p = (a:T)-[e]->(b) RETURN p, id(a) AS a, id(e) AS e, id(b) AS b",
         "dtypes": ["Path", "Int64", "Int64", "Int64"], "check": path_of_ids},

        {"name": "a List column",
         "query": "MATCH (n:T) RETURN [n.name] AS names ORDER BY n.name",
         "dtypes": ["List"], "rows": [[["a"]], [["b"]]]},
        {"name": "a nullable List column",
         "query": "MATCH (n:T) RETURN n.l AS l, labels(n) AS labels ORDER BY n.name",
         "dtypes": ["List", "List"], "rows": [[[1, 2], ["T"]], [None, ["T"]]]},
        {"name": "a List constant",
         "query": "RETURN [1, 2] AS l",
         "dtypes": ["List"], "rows": [[[1, 2]]]},
        {"name": "a nullable List constant",
         "query": "RETURN range(0, 10, 5) AS steps, [[1], [2]][5] AS missing",
         "dtypes": ["List", "List"], "rows": [[[0, 5, 10], None]]},

        {"name": "a Map column",
         "query": "MATCH (n:T) RETURN {name: n.name, b: n.b} AS m ORDER BY n.name",
         "dtypes": ["Map"], "rows": [[{"name": "a", "b": True}], [{"name": "b", "b": None}]]},
        {"name": "a nullable Map column",
         "query": "MATCH (n:T) RETURN n.m AS m ORDER BY n.name",
         "dtypes": ["Map"], "rows": [[{"k": 1, "s": "v", "b": True}], [None]]},
        {"name": "a Map constant",
         "query": "RETURN {a: 1} AS m",
         "dtypes": ["Map"], "rows": [[{"a": 1}]]},

        {"name": "a Cell column",
         "query": "UNWIND [1, 'two'] AS x RETURN x",
         "dtypes": ["Cell"], "rows": [[1], ["two"]]},
        {"name": "a nullable Cell column",
         "query": "MATCH (n:T) RETURN [n.i, n.name][0] AS first ORDER BY n.name",
         "dtypes": ["Cell"], "rows": [[1], [None]]},
        {"name": "a nullable Cell column from an aggregate",
         "query": "MATCH (n:T) WITH {id: n.i} AS m RETURN sum(m.id % 10) AS total",
         "dtypes": ["Cell"], "rows": [[1]]},
        {"name": "a Cell constant",
         "query": "WITH {a: 1} AS m RETURN m.a AS a",
         "dtypes": ["Cell"], "rows": [[1]]},
        {"name": "a nullable Cell constant",
         "query": "WITH [1, 'two'] AS l RETURN l[0] AS first, l[5] AS missing",
         "dtypes": ["Cell", "Cell"], "rows": [[1, None]]},

        {"name": "a label ID and a String column",
         "query": "CALL db.labels()",
         "dtypes": ["UInt64", "String"], "check": one_named("T")},
        {"name": "an edge type ID",
         "query": "CALL db.edgeTypes()",
         "dtypes": ["UInt64", "String"], "check": one_named("R")},
        {"name": "a property type ID and a value type",
         "query": "CALL db.propertyTypes()",
         "dtypes": ["UInt64", "String", "String"], "check": property_types},
        {"name": "a change ID",
         "query": "CHANGE LIST",
         "dtypes": ["UInt64"], "rows": [[change]]},
    ]


def failure(db: turingdb.TuringDB, case: dict) -> str | None:
    query = case["query"]

    try:
        dtypes = list(db.query_raw(query)["dtypes"].values())
        if dtypes != case["dtypes"]:
            return f"types {dtypes}, expected {case['dtypes']}"

        frame = db.query(query)
        columns = [plain(frame[column].tolist()) for column in frame.columns]
        rows = [list(row) for row in zip(*columns)]

        if "rows" in case and rows != case["rows"]:
            return f"rows {rows}, expected {case['rows']}"

        if "check" in case:
            case["check"](rows)
    except AssertionError as e:
        return f"rows {e}"
    except Exception as e:
        return f"{type(e).__name__}: {e}"

    return None


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    failed = 0
    for kind, db in open_clients(script_dir):
        change = db.new_change()
        db.query(GRAPH)
        db.query("COMMIT")

        for case in cases(change):
            problem = failure(db, case)
            if problem is None:
                print(f"  ok   {kind}: {case['name']}")
            else:
                failed += 1
                print(f"  FAIL {kind}: {case['name']}\n    query: {case['query']}\n    {problem}")

    if failed > 0:
        print(f"* value_types: {failed} failed")
        sys.exit(1)

    print("* value_types: done")


if __name__ == "__main__":
    main()
