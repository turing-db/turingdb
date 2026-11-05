import os
from datetime import datetime, timedelta, timezone

import turingdb
from turingdb.exceptions import TuringDBException


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="localhost")
    native.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("embedded", embedded)]


def names(db: turingdb.TuringDB, query: str, query_parameters: dict) -> list[str]:
    return db.query(query, query_parameters=query_parameters)["n.name"].tolist()


def check(kind: str, db: turingdb.TuringDB) -> None:
    db.new_change()
    db.query("CREATE (:Person:Founder {name: 'Remy'}), (:Founder {name: 'Adam'}), (:Person {name: 'Luc'})")
    db.query("COMMIT")

    founders = names(db, "MATCH (n:$label) RETURN n.name ORDER BY n.name", {"label": "Founder"})
    assert founders == ["Adam", "Remy"], f"{kind}: a str label gave {founders}"

    both = names(db, "MATCH (n:$labels) RETURN n.name ORDER BY n.name", {"labels": ["Person", "Founder"]})
    assert both == ["Remy"], f"{kind}: a list of labels gave {both}"

    nested = {"unused": [1, -2.5, True, None, "text", {"key": ["value", {"deep": 3}]}], "label": "Person"}
    people = names(db, "MATCH (n:$label) RETURN n.name ORDER BY n.name", nested)
    assert people == ["Luc", "Remy"], f"{kind}: nested parameters gave {people}"

    matched = names(db, "MATCH (n) WHERE n.name = $name RETURN n.name", {"name": "Luc"})
    assert matched == ["Luc"], f"{kind}: a value parameter gave {matched}"

    computed = db.query("RETURN $i + 1 AS i, $d * 2.0 AS d, $n IS NULL AS n",
                        query_parameters={"i": 41, "d": 1.5, "n": None})
    integer = computed["i"].tolist()
    real = computed["d"].tolist()
    null = computed["n"].tolist()
    assert integer == [42], f"{kind}: an Int64 parameter gave {integer}"
    assert real == [3.0], f"{kind}: a Double parameter gave {real}"
    assert null == [True], f"{kind}: a null parameter gave {null}"

    timed = db.query("RETURN $wait + duration({hours: 1}) AS wait, ($when + $wait).year AS year",
                     query_parameters={"wait": timedelta(hours=1), "when": datetime(2026, 11, 5, 14, 5, tzinfo=timezone.utc)})
    wait = timed["wait"].tolist()
    year = timed["year"].tolist()
    assert wait == [timedelta(hours=2)], f"{kind}: a Duration parameter gave {wait}"
    assert year == [2026], f"{kind}: a DateTime parameter gave {year}"

    try:
        db.query("RETURN $when", query_parameters={"when": datetime(2026, 11, 5, 14, 5)})
    except TuringDBException as error:
        assert "must carry a timezone" in str(error), f"{kind}: a naive datetime gave {error}"
    else:
        raise AssertionError(f"{kind}: a naive datetime parameter was accepted")

    try:
        db.query("MATCH (n:$label) RETURN n")
    except TuringDBException as e:
        assert "Parameter $label is not defined" in str(e), f"{kind}: wrong error {e}"
    else:
        raise AssertionError(f"{kind}: a missing parameter raised no error")

    print(f"{kind}: query parameters work")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for kind, db in open_clients(script_dir):
        check(kind, db)

    print("* query_parameters: done")


if __name__ == "__main__":
    main()
