import os

import turingdb


# '\uD800' is a lone surrogate. The engine stores it as the bytes ED A0 80, which are
# not valid UTF-8, so a client must replace them the way Python's errors="replace" does.
SURROGATE_BYTES = b"\xed\xa0\x80"
REPLACED = SURROGATE_BYTES.decode("utf-8", errors="replace")


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="localhost", port=6666)
    native.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("embedded", embedded)]


def value(db: turingdb.TuringDB, query: str):
    return db.query(query).iloc[0, 0]


def check(kind: str, db: turingdb.TuringDB) -> None:
    literal = value(db, r"RETURN 'a\uD800b' AS s")
    assert literal == f"a{REPLACED}b", f"{kind}: a string literal gave {literal!r}"

    element = value(db, r"RETURN ['x', '\uD800'] AS l")
    assert element == ["x", REPLACED], f"{kind}: a list element gave {element!r}"

    entry = value(db, r"RETURN {k: '\uD800'} AS m")
    assert entry == {"k": REPLACED}, f"{kind}: a map value gave {entry!r}"

    db.new_change()
    db.query(r"CREATE (:Sensor {name: 'radar\uD800'})")
    db.query("COMMIT")

    stored = value(db, "MATCH (n:Sensor) RETURN n.name")
    assert stored == f"radar{REPLACED}", f"{kind}: a stored property gave {stored!r}"

    print(f"{kind}: invalid UTF-8 reads back with U+FFFD in its place")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for kind, db in open_clients(script_dir):
        check(kind, db)

    print("* invalid_utf8_strings: done")


if __name__ == "__main__":
    main()
