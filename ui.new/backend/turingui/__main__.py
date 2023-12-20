from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from turingdb.Request import TuringError
import turingdb
import argparse
import logging
import numpy as np
import pandas as pd
from flask_session import Session


def run(args):
    logging.basicConfig(filename="reports/flask.log", level=logging.DEBUG)
    app = Flask(
        __name__,
        static_url_path="",
        static_folder=args.static,
        template_folder=args.static,
    )

    CORS(app)
    app.config["SESSION_PERMANENT"] = False
    app.config["SESSION_TYPE"] = "filesystem"
    Session(app)

    turing = turingdb.Turing("localhost:6666")
    app.turing = turing

    if args.dev:
        from viewer import viewer_blueprint
    else:
        from turingui.viewer import viewer_blueprint

    app.register_blueprint(viewer_blueprint)

    @app.route("/")
    def index():
        if args.dev:
            return (
                "<h1>ERROR: you're trying to access a "
                "dev server with the wrong port</h1>"
            )
        return render_template("index.html", node_modules_path="../node_modules")

    @app.route("/api/get_status")
    def get_status():
        return jsonify({"running": turing.running})

    @app.route("/api/list_available_databases")
    def list_available_databases():
        try:
            dbs = turing.list_available_databases()
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {
                "failed": False,
                "db_names": list(dbs),
            }
        )

    @app.route("/api/list_loaded_databases")
    def list_loaded_databases():
        try:
            dbs = turing.list_loaded_databases()
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {
                "failed": False,
                "db_names": list(dbs),
            }
        )

    @app.route("/api/is_db_loaded", methods=["GET"])
    def is_db_loaded():
        try:
            loaded_dbs = turing.list_loaded_databases()
            return jsonify({"loaded": request.args.get("db_name") in loaded_dbs})

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

    @app.route("/api/load_database", methods=["POST"])
    def load_database():
        db_name = request.get_json()["db_name"]
        try:
            db = turing.load_db(db_name=db_name)
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {
                "db_name": db_name,
                "db_id": db.id,
            }
        )

    @app.route("/api/list_nodes", methods=["POST"])
    def list_nodes():
        data = request.get_json()
        prop_name = data["prop_name"] if "prop_name" in data else None
        prop_value = data["prop_value"] if "prop_value" in data else None
        node_type_name = data["node_type_name"] if "node_type_name" in data else None
        ids = data.get("ids")
        db_name = data["db_name"]
        exclude_publications = (
            data["exclude_publications"] if "exclude_publications" in data else False
        )
        node_property_filter_out = (
            data["node_property_filter_out"]
            if "node_property_filter_out" in data
            else []
        )
        node_property_filter_in = (
            data["node_property_filter_in"] if "node_property_filter_in" in data else []
        )
        yield_edges = data["yield_edges"] if "yield_edges" in data else False
        nodes = []

        try:
            db = turing.get_db(db_name)
            params = {}

            if ids:
                params["ids"] = [int(id) for id in ids]

                if len(ids) == 0:
                    return jsonify([])

            if node_type_name:
                params["node_type"] = db.list_node_types()[node_type_name]

            if prop_name and prop_value:
                params["property"] = turingdb.Property(prop_name, prop_value)

            if ids and node_type_name is None and prop_name is None:
                nodes = db.list_nodes_by_id(**params)
            else:
                nodes = db.list_nodes(**params)

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        if exclude_publications:
            publication_filter = list(
                filter(lambda node: "Publication" not in node.node_type.name, nodes)
            )

            def author_filter_func(node):
                in_edge_types = node.node_type.in_edge_types.keys()
                return "author" not in in_edge_types

            nodes = list(
                filter(
                    author_filter_func,
                    publication_filter,
                )
            )

        def filter_out_props(node):
            props = node.list_properties()
            for p_name, p_value in node_property_filter_out:
                if p_name not in props:
                    continue
                if props[p_name].value == p_value:
                    return False
            return True

        def filter_in_props(node):
            props = node.list_properties()
            for p_name, p_value in node_property_filter_in:
                if p_name not in props:
                    continue
                if props[p_name].value != p_value:
                    return False
            return True

        nodes = list(filter(filter_out_props, nodes))

        nodes = list(filter(filter_in_props, nodes))

        base_nodes = [
            {
                "id": n.id,
                "node_type": n.node_type.name,
                "properties": {
                    p_name: p_value.value
                    for p_name, p_value in n.list_properties().items()
                },
            }
            for n in nodes
        ]

        if not yield_edges:
            return jsonify({"nodes": base_nodes})

        edges = [
            {
                "ins": n.in_edge_ids,
                "outs": n.out_edge_ids,
            }
            for n in nodes
        ]

        return jsonify(
            {"nodes": [{**base_nodes[i], **edges[i]} for i in range(len(nodes))]}
        )

    @app.route("/api/list_node_property_types", methods=["POST"])
    def list_node_property_types():
        data = request.get_json()
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name)
            property_types = []

            for nt in db.list_node_types().values():
                for pt_name, pt in nt.list_property_types().items():
                    property_types.append(pt_name)

            property_types = set(property_types)

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify({"data": [p_name for p_name in property_types]})

    @app.route("/api/list_edge_property_types", methods=["POST"])
    def list_edge_property_types():
        data = request.get_json()
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name)
            property_types = []

            for nt in db.list_edge_types().values():
                for pt_name, pt in nt.list_property_types().items():
                    property_types.append(pt_name)

            property_types = set(property_types)

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify([p_name for p_name in property_types])

    @app.route("/api/list_node_properties", methods=["POST"])
    def list_node_properties():
        data = request.get_json()
        db_name = data["db_name"]
        id = data["id"] if "id" in data else None

        try:
            db = turing.get_db(db_name)
            node = db.list_nodes(ids=[int(id)])[0]

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {p_name: p.value for p_name, p in node.list_properties().items()}
        )

    @app.route("/api/list_nodes_properties", methods=["POST"])
    def list_nodes_properties():
        data = request.get_json()
        db_name = data["db_name"]
        ids = data.get("ids")

        try:
            db = turing.get_db(db_name)
            nodes = db.list_nodes([int(id) for id in ids])

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {
                node.id: {
                    p_name: p.value for p_name, p in node.list_properties().items()
                }
                for node in nodes
            }
        )

    @app.route("/api/list_connecting_edges", methods=["POST"])
    def list_connecting_edges():
        data = request.get_json()
        db_name = data.get("db_name")
        node_ids = [int(id) for id in data.get("node_ids", [])]

        if len(node_ids) == 0:
            return jsonify([])

        db = turing.get_db(db_name)
        nodes = db.list_nodes(ids=node_ids)
        edge_ids = pd.Series(
            [id for n in nodes for id in n.in_edge_ids + n.out_edge_ids]
        )
        counts = edge_ids.value_counts()
        edge_ids = counts[counts > 1].index.tolist()

        if len(edge_ids) == 0:
            return jsonify([])

        edges = db.list_edges_by_id(ids=edge_ids)

        return jsonify(
            [
                {
                    "id": e.id,
                    "edge_type_name": e.edge_type.name,
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                }
                for e in edges
            ]
        )

    def sliceIds(array, lim):
        if lim == 0:
            return array
        return array if len(array) < lim else array[: lim + 1]

    @app.route("/api/list_neighboring_nodes", methods=["POST"])
    def list_neighboring_nodes():
        data = request.get_json()
        db_name = data.get("db_name")
        node_ids = [int(id) for id in data.get("node_ids", [])]
        edge_count_limit = data.get("edge_count_limit", 30)
        exclude_edge_types = data.get("exclude_edge_types", [])
        exclude_node_types = data.get("exclude_node_types", [])
        select_properties = data.get("select_properties", {})
        hidden_edges = [int(id) for id in data.get("hidden_edges", [])]

        if len(node_ids) == 0:
            return jsonify({"nodes": [], "edges": []})

        db = turing.get_db(db_name)
        nodes = db.list_nodes(ids=node_ids)
        edge_ids = list(
            set(
                [
                    id
                    for n in nodes
                    for id in sliceIds(n.in_edge_ids + n.out_edge_ids, edge_count_limit)
                ]
            )
        )
        edge_ids = list(filter(lambda id: id not in hidden_edges, edge_ids))

        if len(edge_ids) == 0:
            return jsonify({"nodes": [], "edges": []})

        edges = db.list_edges_by_id(ids=edge_ids)

        def edges_to_pandas(edges):
            return pd.DataFrame(
                {
                    "id": [e.id for e in edges],
                    "edge_type_name": [e.edge_type.name for e in edges],
                    "source_id": [e.source_id for e in edges],
                    "target_id": [e.target_id for e in edges],
                }
            )

        node_ids_np = np.array(node_ids)
        edges = edges_to_pandas(edges)
        edges = edges[
            ~edges.source_id.isin(node_ids_np) | ~edges.target_id.isin(node_ids_np)
        ]
        edges = edges[~edges.edge_type_name.isin(exclude_edge_types)]
        neighboring_node_ids = list(
            set(edges[["source_id", "target_id"]].to_numpy().flatten().tolist())
        )

        neighboring_node_ids = list(
            filter(lambda id: id not in node_ids, neighboring_node_ids)
        )
        neighboring_nodes = db.list_nodes(ids=neighboring_node_ids)
        neighboring_nodes = list(
            filter(
                lambda n: all(
                    exclude not in n.node_type.name for exclude in exclude_node_types
                ),
                neighboring_nodes,
            )
        )

        neighboring_nodes = filter(
            lambda n: all(
                True
                if prop_name not in n.properties.keys()
                else n.properties[prop_name].value == prop_value
                for prop_name, prop_value in select_properties.items()
            ),
            neighboring_nodes,
        )

        neighboring_nodes = list(neighboring_nodes)
        all_node_ids = [int(n.id) for n in neighboring_nodes] + node_ids

        edges = edges[edges.source_id.isin(all_node_ids)]
        edges = edges[edges.target_id.isin(all_node_ids)]

        return jsonify(
            {
                "nodes": [
                    {
                        "id": n.id,
                        "node_type": n.node_type.name,
                        "properties": {
                            p_name: p_value.value
                            for p_name, p_value in n.list_properties().items()
                        },
                    }
                    for n in neighboring_nodes
                ],
                "edges": [
                    {
                        "id": e.id,
                        "edge_type_name": e.edge_type_name,
                        "source_id": e.source_id,
                        "target_id": e.target_id,
                    }
                    for _, e in edges.iterrows()
                ],
            }
        )

    @app.route("/api/list_edges", methods=["POST"])
    def list_edges():
        data = request.get_json()
        db_name = data["db_name"]
        ids = data.get("ids", [])
        excludes = data.get("excludes", [])

        try:
            db = turing.get_db(db_name)

            if ids:
                edges = db.list_edges_by_id([int(id) for id in ids])
            else:
                edges = []

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        edges = list(
            filter(
                lambda e: all(exclude not in e.edge_type.name for exclude in excludes),
                edges,
            )
        )

        return jsonify(
            [
                {
                    "id": e.id,
                    "edge_type_name": e.edge_type.name,
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                }
                for e in edges
            ]
        )

    @app.route("/api/list_node_types", methods=["POST"])
    def list_node_types():
        data = request.get_json()
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name)
            node_types = db.list_node_types()
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify([name for name in node_types.keys()])

    @app.route("/api/get_database", methods=["POST"])
    def get_database():
        data = request.get_json()
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name=db_name)
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            {
                "db_name": db_name,
                "db_id": db.id,
            }
        )

    app.run(debug=True, host="0.0.0.0")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="turingui",
        description="Starts the Turing UI server",
    )

    parser.add_argument("static", type=str, help="path to the static files")
    parser.add_argument("--dev", action="store_true", help="Starts a dev server")
    args = parser.parse_args()
    run(args)
