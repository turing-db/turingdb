import os

import turingdb


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="http://localhost:6666")
    native.try_reach()
    json = turingdb.TuringDB(type="json", host="http://localhost:6676")
    json.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("json", json), ("embedded", embedded)]


def column(db: turingdb.TuringDB, query: str, name: str) -> list:
    return db.query(query)[name].tolist()


def check(kind: str, db: turingdb.TuringDB) -> None:
    db.new_change()
    db.query("CREATE (:V {id: 'A'})-[:E]->(:V {id: 'B'})-[:E]->(:V {id: 'C'})")
    db.query("COMMIT")

    nodes = column(db, "MATCH (n:V) RETURN n ORDER BY n.id", "n")
    edges = column(db, "MATCH (a:V)-[e:E]->() RETURN e ORDER BY a.id", "e")

    collected = column(db, "MATCH (n:V) WITH n ORDER BY n.id RETURN collect(n) AS ns", "ns")
    assert collected == [nodes], f"{kind}: collect(n) gave {collected}, expected {[nodes]}"

    single = column(db, "MATCH (n:V {id: 'A'}) RETURN [n] AS ns", "ns")
    assert single == [[nodes[0]]], f"{kind}: [n] gave {single}, expected {[[nodes[0]]]}"

    collected_edges = column(db, "MATCH (a:V)-[e:E]->() WITH a, e ORDER BY a.id RETURN collect(e) AS es", "es")
    assert collected_edges == [edges], f"{kind}: collect(e) gave {collected_edges}, expected {[edges]}"

    walk = "MATCH p = (:V {id: 'A'})-[:E*2]->() RETURN nodes(p) AS ns, relationships(p) AS rs"
    walk_nodes = column(db, walk, "ns")
    walk_edges = column(db, walk, "rs")
    assert walk_nodes == [nodes], f"{kind}: nodes(p) gave {walk_nodes}, expected {[nodes]}"
    assert walk_edges == [edges], f"{kind}: relationships(p) gave {walk_edges}, expected {[edges]}"

    mapped = column(db, "MATCH (n:V {id: 'A'})-[e:E]->() RETURN {node: n, edge: e} AS m", "m")
    assert mapped == [{"node": nodes[0], "edge": edges[0]}], f"{kind}: a map of n and e gave {mapped}"

    print(f"{kind}: nodes and edges in lists and maps read as their IDs")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for kind, db in open_clients(script_dir):
        check(kind, db)

    print("* entity_list_values: done")


if __name__ == "__main__":
    main()
