from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from turingdb.Request import TuringError
import turingdb
import argparse
import logging


def run(args):
    logging.basicConfig(filename="flask.log", level=logging.DEBUG)
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

    @app.route("/api/list_nodes", methods=["GET"])
    def list_nodes():
        prop_name = request.args.get("property_name")
        prop_value = request.args.get("property_value")
        node_type_name = request.args.get("node_type_name")
        ids = request.args.get("ids")
        db_name = request.args.get("db_name")
        try:
            db = turing.get_db(db_name)
            params = {}

            if ids:
                params["ids"] = [int(id) for id in ids.split(",")]

            if node_type_name:
                params["node_type"] = db.list_node_types()[node_type_name]

            nodes = db.list_nodes(**params)

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            [
                {
                    "id": n.id,
                    "ins": n.in_edge_ids,
                    "outs": n.out_edge_ids,
                } for n in nodes
            ]
        )

    @app.route("/api/list_edges", methods=["GET"])
    def list_edges():
        db_name = request.args.get("db_name")
        ids = request.args.get("ids")

        try:
            db = turing.get_db(db_name)

            if ids:
                edges = db.list_edges_by_id([int(id) for id in ids.split(',')])
            else:
                edges = []

        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify(
            [
                {
                    "id": e.id,
                    "source_id": e.target_id,
                    "target_id": e.source_id,
                }
                for e in edges
            ]
        )

    @app.route("/api/list_node_types", methods=["GET"])
    def list_node_types():
        db_name = request.args.get("db_name")

        try:
            db = turing.get_db(db_name)
            node_types = db.list_node_types()
        except TuringError as err:
            return jsonify({"failed": True, "error": {"details": err.details}})

        return jsonify([name for name in node_types.keys()])

    @app.route("/api/get_database", methods=["POST"])
    def get_database():
        db_name = request.get_json()["db_name"]
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
