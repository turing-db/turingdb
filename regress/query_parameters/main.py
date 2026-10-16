import os

import turingdb
from turingdb.exceptions import TuringDBException


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="http://localhost:6666")
    native.try_reach()
    json = turingdb.TuringDB(type="json", host="http://localhost:6676")
    json.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("json", json), ("embedded", embedded)]


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
