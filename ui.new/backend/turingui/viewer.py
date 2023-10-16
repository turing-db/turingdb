from typing import Mapping
from flask import jsonify, Blueprint, current_app, request, session

import turingdb
import copy
import pandas as pd
import time

viewer_blueprint = Blueprint("viewer", __name__)


class ViewerSession:
    def __init__(self):
        self.turing: turingdb.Turing = None
        self.elements = []
        self.db_name = ""
        self.db: turingdb.core.Database = None
        self.edge_count_lim = 30
        self.t0 = time.time()
        self.hidden_node_ids = []


def restart_timer():
    session.viewer.t0 = time.time()


def log_time(name):
    current_app.logger.info(f"{name}: {time.time() - session.viewer.t0:.4f} s")


def appearing_more_than_once(ids: list[int]) -> list[int]:
    counts = pd.Series(ids).value_counts()
    return counts[counts > 1].index.tolist()


def filter_edges(e: turingdb.core.Edge, node_ids: list[int]):
    return e.source_id in node_ids and e.target_id in node_ids


class Edge:
    def __init__(self, id, source_id: int, target_id: int):
        self.id = id
        self.source_id = source_id
        self.target_id = target_id


def make_network(selected_nodes: Mapping[int, turingdb.core.Node]):
    selected_node_ids = list(selected_nodes.keys())
    selected_nodes = selected_nodes.values()

    restart_timer()
    for n in selected_nodes:
        n.edge_definitions = pd.Series(n.in_edge_definitions + n.out_edge_definitions)
        n.edge_definitions_df = pd.DataFrame(
            {
                "source_ids": [ed.source_id for ed in n.edge_definitions],
                "target_ids": [ed.target_id for ed in n.edge_definitions],
            },
            index=[ed.id for ed in n.edge_definitions],
        )
        n.selected_in_connections = pd.Series(n.in_node_ids)
        n.selected_in_connections = n.selected_in_connections[
            n.selected_in_connections.isin(selected_node_ids)
        ]
        n.selected_out_connections = pd.Series(n.out_node_ids)
        n.selected_out_connections = n.selected_out_connections[
            n.selected_out_connections.isin(selected_node_ids)
        ]
        n.selected_connections = pd.Series(
            n.selected_in_connections.tolist() + n.selected_out_connections.tolist()
        ).unique()
        n.is_important = n.selected_connections.size > 2
    log_time("making edge definitions in nodes")

    restart_timer()
    all_edge_definitions = pd.DataFrame(
        {
            "source_ids": [
                ed.source_id for n in selected_nodes for ed in n.edge_definitions
            ],
            "target_ids": [
                ed.target_id for n in selected_nodes for ed in n.edge_definitions
            ],
        },
        index=[ed.id for n in selected_nodes for ed in n.edge_definitions],
    )
    log_time("making edge definitions")

    # This stores all edges into an easy to use pd dataframe
    restart_timer()
    all_edge_definitions = (
        all_edge_definitions.reset_index()
        .drop_duplicates(subset="index", keep="last")
        .set_index("index")
    )
    log_time("all_edge_definitions")

    # All neighbor edges
    restart_timer()
    all_neighbor_edge_definitions = all_edge_definitions[
        ~all_edge_definitions.source_ids.isin(selected_node_ids)
        | ~all_edge_definitions.target_ids.isin(selected_node_ids)
    ]
    log_time("all_neighbor_edge_definitions")

    # This stores edges between selected nodes
    restart_timer()
    connecting_edge_definitions = all_edge_definitions[
        ~all_edge_definitions.index.isin(all_neighbor_edge_definitions.index)
    ]
    log_time("connecting_edge_definitions")

    restart_timer()
    connecting_edge_ids = connecting_edge_definitions.index.tolist()
    log_time("connecting_edge_ids")

    # Nodes that have too many edges
    restart_timer()
    all_node_ids = pd.Series(
        all_neighbor_edge_definitions.source_ids.tolist()
        + all_neighbor_edge_definitions.target_ids.tolist()
    )
    all_neighbor_node_ids = all_node_ids[~all_node_ids.isin(selected_node_ids)]
    important_neighbor_ids = pd.Series([n.id for n in selected_nodes if n.is_important])

    # This stores the ids of neighbors that are connected at least twice (not selected)
    # ie, Node ids that we need to keep because important although node might be bloated
    restart_timer()
    important_neighbor_ids = important_neighbor_ids[
        ~important_neighbor_ids.isin(selected_node_ids)
    ].tolist()
    log_time("important_neighbor_ids")

    restart_timer()
    important_edge_ids = all_neighbor_edge_definitions[
        all_neighbor_edge_definitions.source_ids.isin(important_neighbor_ids)
        | all_neighbor_edge_definitions.target_ids.isin(important_neighbor_ids)
    ].index.tolist()
    log_time("important_edge_ids")

    restart_timer()
    filtered_edge_ids = (
        pd.Series(
            [
                id
                for n in selected_nodes
                for id in n.edge_definitions_df[
                    ~n.edge_definitions_df.index.isin(important_edge_ids)
                ].index
            ]
            + important_edge_ids
        )
        .unique()
        .tolist()
    )
    log_time("filtered_edge_ids")

    restart_timer()
    filtered_edge_definitions = all_edge_definitions[
        all_edge_definitions.index.isin(filtered_edge_ids)
    ]
    log_time("filtered_edge_definitions")

    restart_timer()
    neighbor_edge_definitions = filtered_edge_definitions[
        ~filtered_edge_definitions.index.isin(connecting_edge_ids)
        & ~filtered_edge_definitions.source_ids.isin(session.viewer.hidden_node_ids)
        & ~filtered_edge_definitions.target_ids.isin(session.viewer.hidden_node_ids)
    ]
    log_time("neighbor_edge_definitions")

    restart_timer()
    neighbor_edge_ids = neighbor_edge_definitions.index.tolist()
    neighbor_node_ids = (
        all_neighbor_node_ids[
            all_node_ids.isin(neighbor_edge_definitions.source_ids)
            | all_node_ids.isin(neighbor_edge_definitions.target_ids)
        ]
        .unique()
        .tolist()
    )
    log_time("neighbor_node_ids")

    restart_timer()
    neighbor_nodes = session.viewer.db.list_nodes_by_id(
        ids=neighbor_node_ids, yield_edges=False
    )
    log_time("neigbor_nodes")
    restart_timer()
    neighbor_edges = session.viewer.db.list_edges_by_id(ids=neighbor_edge_ids)

    log_time("neighbor_edges")
    restart_timer()
    connecting_edges = session.viewer.db.list_edges_by_id(ids=connecting_edge_ids)
    log_time("connecting_edges")

    selected_node_elements = [
        {
            "data": {
                "id": n.id,
                "node_type_name": n.node_type.name,
                "properties": {
                    p_name: p_value.value for p_name, p_value in n.properties.items()
                },
                "type": "selected",
                "bloated": n.in_edge_count + n.out_edge_count
                > session.viewer.edge_count_lim,
                "in_edge_count": n.in_edge_count,
                "out_edge_count": n.out_edge_count,
            },
            "group": "nodes",
        }
        for n in selected_nodes
    ]

    neighbor_node_elements = [
        {
            "data": {
                "id": n.id,
                "node_type_name": n.node_type.name,
                "properties": {
                    p_name: p_value.value for p_name, p_value in n.properties.items()
                },
                "type": "neighbor",
                "bloated": n.in_edge_count + n.out_edge_count
                > session.viewer.edge_count_lim,
                "in_edge_count": n.in_edge_count,
                "out_edge_count": n.out_edge_count,
            },
            "group": "nodes",
        }
        for n in neighbor_nodes
    ]

    connecting_edge_elements = [
        {
            "data": {
                "id": -e.id - 1,
                "edge_type_name": e.edge_type.name,
                "properties": {
                    p_name: p_value.value for p_name, p_value in e.properties.items()
                },
                "source": e.source_id,
                "target": e.target_id,
                "type": "connecting",
            },
            "group": "edges",
        }
        for e in connecting_edges
    ]

    neighboring_edge_elements = [
        {
            "data": {
                "id": -e.id - 1,
                "edge_type_name": e.edge_type.name,
                "properties": {
                    p_name: p_value.value for p_name, p_value in e.properties.items()
                },
                "source": e.source_id,
                "target": e.target_id,
                "type": "neighbor",
            },
            "group": "edges",
        }
        for e in neighbor_edges
    ]

    session.viewer.elements = (
        selected_node_elements
        + neighbor_node_elements
        + connecting_edge_elements
        + neighboring_edge_elements
    )


@viewer_blueprint.route("/api/viewer/init", methods=["POST"])
def init():
    t0 = time.time()
    data = request.get_json()
    node_ids = [int(id) for id in data.get("node_ids", [])]
    db_name = data.get("db_name")
    hidden_node_ids = [int(id) for id in data.get("hidden_node_ids", [])]
    edge_count_lim = data.get("edge_count_lim", 30)
    node_property_filter_in = data.get("node_property_filter_in", [])
    node_property_filter_out = data.get("node_property_filter_out", [])


    if not db_name:
        return jsonify({"Error": "You need to provide a db_name to start"})

    session.viewer = ViewerSession()
    session.viewer.db_name = db_name
    session.viewer.turing: turingdb.Turing = current_app.turing
    session.viewer.db = session.viewer.turing.get_db(db_name)
    session.viewer.hidden_node_ids = hidden_node_ids
    session.viewer.edge_count_lim = edge_count_lim

    if len(node_ids) == 0:
        return session.viewer.elements

    nodes = session.viewer.db.list_nodes_by_id(
        ids=node_ids,
        yield_edges=True,
        max_edge_count=session.viewer.edge_count_lim,
        node_property_filter_out=[
            turingdb.Property(propName, propValue, turingdb.ValueType.STRING)
            for propName, propValue in node_property_filter_out
        ],
        node_property_filter_in=[
            turingdb.Property(propName, propValue, turingdb.ValueType.STRING)
            for propName, propValue in node_property_filter_in
        ],
    )

    make_network({n.id: n for n in nodes})

    current_app.logger.info(f"Total time of /viewer/init: {time.time() - t0:.4f} s")
    return jsonify(session.viewer.elements)


@viewer_blueprint.route("/api/viewer/hide_neighbors", methods=["POST"])
def hide_neighbors():
    data = request.get_json()
    node_ids = [int(id) for id in data.get("node_ids", [])]
    session.viewer.hidden_neighbor_ids = list(
        set(session.viewer.hidden_neighbor_ids + node_ids)
    )


@viewer_blueprint.route("/api/viewer/clear", methods=["POST"])
def clear():
    db_name = copy.deepcopy(session.viewer.db_name)
    db = copy.deepcopy(session.viewer.db)
    session.viewer = ViewerSession()
    session.viewer.db_name = db_name
    session.viewer.db = db
