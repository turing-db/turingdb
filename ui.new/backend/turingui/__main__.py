from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from turingdb.Request import TuringError
import turingdb
import argparse
import logging


def run(args):
    logging.basicConfig(filename="reports/flask.log", level=logging.DEBUG)
    app = Flask(
        __name__,
        static_url_path="",
        static_folder=args.static,
        template_folder=args.static,
    )
    CORS(app)

    turing = turingdb.Turing("localhost:6666")

    @app.route("/")
    def index():
        if args.dev:
            return (
                "<h1>ERROR: you're trying to access a "
                "dev server with the wrong port</h1>"
            )
        return render_template("index.html")

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
        ids = data["ids"] if "ids" in data else None
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name)
            params = {}

            if ids:
                params["ids"] = [int(id) for id in ids]

            if node_type_name:
                params["node_type"] = db.list_node_types()[node_type_name]

            if prop_name and prop_value:
                params["property"] = turingdb.Property(prop_name, prop_value)

            nodes = db.list_nodes(**params)

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            [
                {
                    "id": n.id,
                    "ins": n.in_edge_ids,
                    "outs": n.out_edge_ids,
                }
                for n in nodes
            ]
        )

    @app.route("/api/list_string_property_types", methods=["POST"])
    def list_string_property_types():
        data = request.get_json()
        db_name = data["db_name"]

        try:
            db = turing.get_db(db_name)
            property_types = []

            for nt in db.list_node_types().values():
                for pt_name, pt in nt.list_property_types().items():
                    if pt.value_type == turingdb.ValueType.STRING.value:
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
        ids = data["ids"] if "ids" in data else None

        try:
            db = turing.get_db(db_name)
            nodes = db.list_nodes([int(id) for id in ids])

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify({
                node.id: {p_name: p.value for p_name, p in node.list_properties().items()}
                for node in nodes
        })

    @app.route("/api/list_edges", methods=["POST"])
    def list_edges():
        data = request.get_json()
        db_name = data["db_name"]
        ids = data["ids"] if "ids" in data else None

        try:
            db = turing.get_db(db_name)

            if ids:
                edges = db.list_edges_by_id([int(id) for id in ids])
            else:
                edges = []

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

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
