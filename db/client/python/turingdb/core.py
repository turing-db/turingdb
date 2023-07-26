from __future__ import annotations
from turingdb.Request import Request

from typing import TYPE_CHECKING, Mapping
if TYPE_CHECKING:
    from turingdb.Turing import Turing

import turingdb.proto.DBService_pb2 as dbService_pb2
import enum


class DBObject:
    def __init__(self, id: int):
        self._id = id

    @property
    def id(self) -> int:
        return self._id


class Database(DBObject):
    def __init__(self, turing: Turing, db):
        super().__init__(db.id)
        self._name = db.name
        self._turing = turing
        self._networks = {}

    def dump(self) -> None:
        Request(self._turing, "DumpDB", db_id=self._id)

    def unload(self) -> None:
        Request(self._turing, "UnloadDB", db_name=self._name)

    def _get_network(self, id: int) -> Network:
        if id not in self._networks:
            self.list_networks()

        return self._networks[id]

    def get_network(self, name: str) -> Network:
        return self.list_networks()[name]

    def create_network(self, name: str) -> Network:
        res = Request(self._turing, "CreateNetwork", db_id=self._id, name=name)
        net = Network(self, res.data.network)
        self._networks[res.data.network.id] = net
        return net

    def create_node_type(self, name: str) -> NodeType:
        res = Request(self._turing, "CreateNodeType", db_id=self._id, name=name)
        return NodeType(self, res.data.node_type)

    def create_edge_type(
        self, name: str, sources: list[NodeType], targets: list[NodeType]
    ) -> EdgeType:
        source_ids = [nt.id for nt in sources]
        target_ids = [nt.id for nt in targets]
        res = Request(
            self._turing,
            "CreateEdgeType",
            db_id=self.id,
            name=name,
            source_ids=source_ids,
            target_ids=target_ids,
        )
        return EdgeType(self, res.data.edge_type)

    def list_node_types(self) -> Mapping[str, NodeType]:
        res = Request(self._turing, "ListNodeTypes", db_id=self._id)
        return {nt.name: NodeType(self, nt) for nt in res.data.node_types}

    def list_node_types_by_id(self, ids: list[int]) -> Mapping[str, NodeType]:
        res = Request(self._turing, "ListNodeTypesByID", db_id=self._id, node_type_ids=ids)
        return {nt.name: NodeType(self, nt) for nt in res.data.node_types}

    def list_edge_types(self) -> Mapping[str, EdgeType]:
        res = Request(self._turing, "ListEdgeTypes", db_id=self._id)
        return {et.name: EdgeType(self, et) for et in res.data.edge_types}

    def list_edge_types_by_id(self, ids: list[int]) -> Mapping[str, EdgeType]:
        res = Request(self._turing, "ListEdgeTypesByID", db_id=self._id, edge_type_ids=ids)
        return {et.name: EdgeType(self, et) for et in res.data.edge_types}

    def list_networks(self) -> Mapping[str, Network]:
        """List the networks of a DB and caches them in the Database class"""
        res = Request(self._turing, "ListNetworks", db_id=self._id)
        networks = {net.name: None for net in res.data.networks}
        for net in res.data.networks:
            self._networks[net.id] = Network(self, net)
            networks[net.name] = Network(self, net)
        return networks

    def create_edge(self, edge_type: EdgeType, source: Node, target: Node) -> Edge:
        res = Request(
            self._turing,
            "CreateEdge",
            db_id=self.id,
            edge_type_id=edge_type.id,
            source_id=source.id,
            target_id=target.id,
        )
        return Edge(self, res.data.edge)

    def list_nodes(self, ids: list[int]=[], node_type: NodeType=None, property: Property = None) -> list[Node]:
        kwargs = {
            "db_id": self.id
        }
        if len(ids) != 0:
            kwargs["filter_id"] = dbService_pb2.FilterID(
                ids=ids
            )
        if node_type != None:
            kwargs["filter_type"] = dbService_pb2.FilterType(
                node_type_id = node_type.id
            )

        if property != None:
            if property.value_type.value == dbService_pb2.ValueType.INT:
                oneof = "int64"
            elif property.value_type.value == dbService_pb2.ValueType.UNSIGNED:
                oneof = "uint64"
            elif property.value_type.value == dbService_pb2.ValueType.STRING:
                oneof = "string"
            elif property.value_type.value == dbService_pb2.ValueType.BOOL:
                oneof = "bool"
            elif property.value_type.value == dbService_pb2.ValueType.DECIMAL:
                oneof = "decimal"

            kwargs["filter_property"] = dbService_pb2.FilterProperty(
                property=dbService_pb2.Property(
                    property_type_name=property.name,
                    value_type=property.value_type.value, **{oneof: property.value}
                )
            )
        res = Request(self._turing, "ListNodes", **kwargs)
        return [Node(self._get_network(n.net_id), n) for n in res.data.nodes]

    def list_nodes_by_id(self, ids: list[int]) -> list[Node]:
        res = Request(self._turing, "ListNodesByID", db_id=self.id, node_ids=ids)
        return [Node(self._get_network(n.net_id), n) for n in res.data.nodes]

    def list_edges(self) -> list[Edge]:
        res = Request(self._turing, "ListEdges", db_id=self.id)
        return [Edge(self, e) for e in res.data.edges]

    def list_edges_by_id(self, ids: list[int]) -> list[Edge]:
        res = Request(self._turing, "ListEdgesByID", db_id=self.id, edge_ids=ids)
        return [Edge(self, e) for e in res.data.edges]

    def __repr__(self) -> str:
        return f'<Database "{self._name}" id={self.id}>'


class ValueType(enum.Enum):
    INT = dbService_pb2.ValueType.INT
    UNSIGNED = dbService_pb2.ValueType.UNSIGNED
    BOOL = dbService_pb2.ValueType.BOOL
    STRING = dbService_pb2.ValueType.STRING
    DECIMAL = dbService_pb2.ValueType.DECIMAL

    def __repr__(self) -> str:
        return f'<ValueType {self.value}>'


class EntityType(DBObject):
    def __init__(
        self, db: Database, id: int, name: str, entity_type: dbService_pb2.EntityType
    ):
        super().__init__(id)
        self._db = db
        self._name = name
        self._entity_type = entity_type

    def list_property_types(self) -> Mapping[str, PropertyType]:
        res = Request(
            self._db._turing,
            "ListPropertyTypes",
            db_id=self._db.id,
            entity_type_id=self._id,
            entity_type=self._entity_type,
        )
        return {pt.name: PropertyType(pt) for pt in res.data.property_types}

    def add_property_type(
        self, name: str, value_type: ValueType
    ) -> PropertyType:
        res = Request(
            self._db._turing,
            "AddPropertyType",
            db_id=self._db.id,
            entity_type_id=self.id,
            entity_type=self._entity_type,
            property_type=dbService_pb2.PropertyType(
                name=name, value_type=value_type.value
            ),
        )
        return PropertyType(res.data.property_type)

    def __repr__(self) -> str:
        return f'<EntityType "{self._name}" id={self.id}>'

    @property
    def name(self) -> str:
        return self._name


class NodeType(EntityType):
    def __init__(self, db: Database, nt):
        super().__init__(db, nt.id, nt.name, dbService_pb2.EntityType.NODE)

    def __repr__(self) -> str:
        return f'<NodeType "{self._name}" id={self.id}>'


class EdgeType(EntityType):
    def __init__(self, db: Database, et):
        super().__init__(db, et.id, et.name, dbService_pb2.EntityType.EDGE)

    def __repr__(self) -> str:
        return f'<EdgeType "{self._name}" id={self.id}>'


class Network(DBObject):
    def __init__(self, db: Database, net):
        super().__init__(net.id)
        self._db = db
        self._name = net.name

    def create_node(self, node_type: NodeType, name: str = "") -> Node:
        res = Request(
            self._db._turing,
            "CreateNode",
            db_id=self._db.id,
            net_id=self.id,
            name=name,
            node_type_id=node_type.id,
        )
        return Node(self, res.data.node)

    def list_nodes(self) -> list[Node]:
        res = Request(
            self._db._turing, "ListNodesFromNetwork", db_id=self._db.id, net_id=self.id
        )
        return [Node(self, n) for n in res.data.nodes]

    def list_nodes_by_id(self, ids: list[int]) -> list[Node]:
        res = Request(
            self._db._turing, "ListNodesByIDFromNetwork", db_id=self._db.id, net_id=self.id, node_ids=ids
        )
        return [Node(self, n) for n in res.data.nodes]

    def list_edges(self) -> list[Edge]:
        res = Request(
            self._db._turing, "ListEdgesFromNetwork", db_id=self._db.id, net_id=self.id
        )
        return [Edge(self._db, e) for e in res.data.edges]

    def list_edges_by_id(self, ids: list[int]) -> list[Edge]:
        res = Request(
            self._db._turing, "ListEdgesByIDFromNetwork", db_id=self._db.id, net_id=self.id, edge_ids=ids
        )
        return [Edge(self._db, e) for e in res.data.edges]

    def __repr__(self) -> str:
        return f'<Network "{self._name}" id={self.id}>'


class PropertyType:
    def __init__(self, pt):
        self._name = pt.name
        self._value_type = pt.value_type

    @property
    def value_type(self) -> int:
        return self._value_type

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f'<PropertyType "{self._name}" value_type={self._value_type}>'


class Property:
    def __init__(self, name: str, value, value_type: ValueType = ValueType.STRING):
        self._name = name
        self._value = value
        self._value_type = value_type

    @property
    def value(self):
        return self._value

    @property
    def value_type(self):
        return self._value_type

    @property
    def name(self):
        return self._name

    def __repr__(self) -> str:
        return f'<Property "{self._name}" value={self._value}>'


class Entity(DBObject):
    def __init__(self, id: int, db: Database, entity_type: dbService_pb2.EntityType):
        super().__init__(id)
        self._db = db
        self._entity_type = entity_type

    def list_properties(self) -> Mapping[str, Property]:
        res = Request(
            self._db._turing,
            "ListEntityProperties",
            db_id=self._db.id,
            entity_type=self._entity_type,
            entity_id=self.id,
        )
        return {p.property_type_name: Property(
            p.property_type_name,
            getattr(p, p.WhichOneof("value")),
            p.value_type,
        ) for p in res.data.properties}

    def add_property(self, pt: PropertyType, value) -> Property:
        if pt.value_type == dbService_pb2.ValueType.INT:
            oneof = "int64"
        elif pt.value_type == dbService_pb2.ValueType.UNSIGNED:
            oneof = "uint64"
        elif pt.value_type == dbService_pb2.ValueType.STRING:
            oneof = "string"
        elif pt.value_type == dbService_pb2.ValueType.BOOL:
            oneof = "bool"
        elif pt.value_type == dbService_pb2.ValueType.DECIMAL:
            oneof = "decimal"

        res = Request(
            self._db._turing,
            "AddEntityProperty",
            db_id=self._db.id,
            entity_id=self.id,
            entity_type=self._entity_type,
            property=dbService_pb2.Property(
                property_type_name=pt.name, value_type=pt.value_type, **{oneof: value}
            ),
        )
        return Property(
            res.data.property_type_name,
            getattr(res.data.property, res.data.property.WhichOneof("value")),
            res.data.property.value_type,
        )

    def __repr__(self) -> str:
        return f'<Entity id={self.id}>'


class Node(Entity):
    def __init__(self, net: Network, node):
        super().__init__(node.id, net._db, dbService_pb2.EntityType.NODE)
        self._net = net
        self._name = node.name
        self._in_edge_ids = node.in_edge_ids
        self._out_edge_ids = node.out_edge_ids
        self._node_type_id = node.node_type_id

    def __repr__(self) -> str:
        return f'<Node name="{self._name}" id={self.id}>'

    @property
    def name(self) -> str:
        return self._name

    @property
    def in_edge_ids(self) -> list[int]:
        return self._in_edge_ids

    @property
    def in_edges(self) -> list[Edge]:
        return self._db.list_edges_by_id(self._in_edge_ids)

    @property
    def out_edges(self) -> list[Edge]:
        return self._db.list_edges_by_id(self._out_edge_ids)

    @property
    def out_edge_ids(self) -> list[int]:
        return self._out_edge_ids

    @property
    def node_type_id(self) -> int:
        return self._node_type_id

    @property
    def node_type(self) -> EdgeType:
        for _, v in self._db.list_node_types_by_id([self._node_type_id]).items():
            return v
        return None


class Edge(Entity):
    def __init__(self, db: Database, edge):
        super().__init__(edge.id, db, dbService_pb2.EntityType.EDGE)
        self._source_id = edge.source_id
        self._target_id = edge.target_id
        self._edge_type_id = edge.edge_type_id

    def __repr__(self) -> str:
        return f'<Edge id={self.id}>'

    @property
    def source_id(self) -> int:
        return self._source_id

    @property
    def source(self) -> Node:
        return self._db.list_nodes_by_id([self._source_id])[0]

    @property
    def target(self) -> Node:
        return self._db.list_nodes_by_id([self._target_id])[0]

    @property
    def target_id(self) -> int:
        return self._target_id

    @property
    def edge_type_id(self) -> int:
        return self._edge_type_id

    @property
    def edge_type(self) -> EdgeType:
        for _, v in self._db.list_edge_types_by_id([self._edge_type_id]).items():
            return v
        return None

