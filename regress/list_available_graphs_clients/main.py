import os

import turingdb


def open_clients(script_dir: str) -> list[tuple[str, turingdb.TuringDB]]:
    embedded_dir = os.path.join(script_dir, ".turing-embedded")
    os.makedirs(embedded_dir, exist_ok=True)

    native = turingdb.TuringDB(type="native", host="localhost", port=6666)
    native.try_reach()
    embedded = turingdb.TuringDB(type="embedded", data_dir=embedded_dir)

    return [("native", native), ("embedded", embedded)]


def check(kind: str, db: turingdb.TuringDB) -> None:
    db.create_graph("sensors")

    graphs = db.list_available_graphs()
    assert "default" in graphs, f"{kind}: list_available_graphs gave {graphs}"
    assert "sensors" in graphs, f"{kind}: list_available_graphs gave {graphs}"

    print(f"{kind}: list_available_graphs names every graph on disk")


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    for kind, db in open_clients(script_dir):
        check(kind, db)

    print("* list_available_graphs_clients: done")


if __name__ == "__main__":
    main()
