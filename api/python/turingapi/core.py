from __future__ import annotations
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from turingapi import Turing

from turingapi.requests import Response
import turingapi.proto.APIService_pb2 as APIService_pb2
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

    def dump(self) -> Response[None]:
        return Response.get(self._turing, "DumpDB", db_id=self._id)

    def unload(self) -> Response[None]:
        return Response.get(self._turing, "UnloadDB", db_name=self._name)

    def get_network(self, id: int) -> Network | None:
        if id not in self._networks:
            res = self.list_networks()
            if res.failed():
                return None
        return self._networks[id]

    def create_network(self, name: str) -> Response[Network]:
        res = Response.get(self._turing, "CreateNetwork", db_id=self._id, name=name)
        if res.failed():
            return res
        res.data = Network(self, res.data.network)
        self._networks[res.data.id] = res.data
        return res

    def create_node_type(self, name: str) -> Response[NodeType]:
        res = Response.get(self._turing, "CreateNodeType", db_id=self._id, name=name)
        if res.failed():
            return res
        res.data = NodeType(self, res.data.node_type)
        return res

    def create_edge_type(
        self, name: str, sources: list[NodeType], targets: list[NodeType]
    ) -> Response[EdgeType]:
        source_ids = [nt.id for nt in sources]
        target_ids = [nt.id for nt in targets]
        res = Response.get(
            self._turing,
            "CreateEdgeType",
            db_id=self.id,
            name=name,
            source_ids=source_ids,
            target_ids=target_ids,
        )
        if res.failed():
            return res
        res.data = EdgeType(self, res.data.edge_type)
        return res

    def list_node_types(self) -> Response[Mapping[str, NodeType]]:
        res = Response.get(self._turing, "ListNodeTypes", db_id=self._id)
        if res.failed():
            return res
        res.data = {nt.name: NodeType(self, nt) for nt in res.data.node_types}
        return res

    def list_edge_types(self) -> Response[Mapping[str, EdgeType]]:
        res = Response.get(self._turing, "ListEdgeTypes", db_id=self._id)
        if res.failed():
            return res
        res.data = {et.name: EdgeType(self, et) for et in res.data.edge_types}
        return res

    def list_networks(self) -> Response[Mapping[str, Network]]:
        """List the networks of a DB and caches them in the Database class"""
        res = Response.get(self._turing, "ListNetworks", db_id=self._id)
        if res.failed():
            return res
        networks = {net.name: None for net in res.data.networks}
        for net in res.data.networks:
            self._networks[net.id] = Network(self, net)
            networks[net.name] = Network(self, net)
        res.data = networks
        return res

    def create_edge(
        self, edge_type: EdgeType, source: Node, target: Node
    ) -> Response[Edge]:
        res = Response.get(
            self._turing,
            "CreateEdge",
            db_id=self.id,
            edge_type_id=edge_type.id,
            source_id=source.id,
            target_id=target.id,
        )
        if res.failed():
            return res

        res.data = Edge(self, res.data.edge)
        return res

    def list_nodes(self) -> Response[list[Node]]:
        res = Response.get(self._turing, "ListNodes", db_id=self.id)
        if res.failed():
            return res
        res.data = [Node(self.get_network(n.net_id), n) for n in res.data.nodes]
        return res

    def list_edges(self) -> Response[list[Edge]]:
        res = Response.get(self._turing, "ListEdges", db_id=self.id)
        if res.failed():
            return res
        res.data = [
            Edge(
                self,
                e,
            )
            for e in res.data.edges
        ]
        return res


class ValueType(enum.Enum):
    INT = APIService_pb2.ValueType.INT
    UNSIGNED = APIService_pb2.ValueType.UNSIGNED
    BOOL = APIService_pb2.ValueType.BOOL
    STRING = APIService_pb2.ValueType.STRING
    DECIMAL = APIService_pb2.ValueType.DECIMAL


class EntityType(DBObject):
    def __init__(
        self, db: Database, id: int, name: str, entity_type: APIService_pb2.EntityType
    ):
        super().__init__(id)
        self._db = db
        self._name = name
        self._entity_type = entity_type

    def list_property_types(self) -> Response[Mapping[str, PropertyType]]:
        res = Response.get(
            self._db._turing,
            "ListPropertyTypes",
            db_id=self._db.id,
            entity_type_id=self._id,
            entity_type=self._entity_type,
        )
        if res.failed():
            return res
        res.data = {pt.name: PropertyType(pt) for pt in res.data.property_types}
        return res

    def add_property_type(
        self, name: str, value_type: ValueType
    ) -> Response[PropertyType]:
        res = Response.get(
            self._db._turing,
            "AddPropertyType",
            db_id=self._db.id,
            entity_type_id=self.id,
            entity_type=self._entity_type,
            property_type=APIService_pb2.PropertyType(
                name=name, value_type=value_type.value
            ),
        )
        if res.failed():
            return res

        res.data = PropertyType(res.data.property_type)
        return res.data


class NodeType(EntityType):
    def __init__(self, db: Database, nt):
        super().__init__(db, nt.id, nt.name, APIService_pb2.EntityType.NODE)


class EdgeType(EntityType):
    def __init__(self, db: Database, et):
        super().__init__(db, et.id, et.name, APIService_pb2.EntityType.EDGE)


class Network(DBObject):
    def __init__(self, db: Database, net):
        super().__init__(net.id)
        self._db = db
        self._name = net.name

    def create_node(self, node_type: NodeType, name: str = "") -> Response[Node]:
        res = Response.get(
            self._db._turing,
            "CreateNode",
            db_id=self._db.id,
            net_id=self.id,
            name=name,
            node_type_id=node_type.id,
        )
        if res.failed():
            return res
        res.data = Node(self, res.data.node)
        return res

    def list_nodes(self) -> Response[list[Node]]:
        res = Response.get(
            self._db._turing, "ListNodesFromNetwork", db_id=self._db.id, net_id=self.id
        )
        if res.failed():
            return res
        res.data = [Node(self, n) for n in res.data.nodes]
        return res

    def list_edges(self) -> Response[list[Edge]]:
        res = Response.get(
            self._db._turing, "ListEdgesFromNetwork", db_id=self._db.id, net_id=self.id
        )
        if res.failed():
            return res

        res.data = [Edge(self._db, e) for e in res.data.edges]
        return res


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


class Property:
    def __init__(self, p):
        self._name = p.property_type_name
        self._value_type = p.value_type
        self._value = getattr(p, p.WhichOneof('value'))

    @property
    def value(self):
        return self._value


class Entity(DBObject):
    def __init__(self, id: int, db: Database, entity_type: APIService_pb2.EntityType):
        super().__init__(id)
        self._db = db
        self._entity_type = entity_type

    def list_properties(self) -> Response(Mapping[str, Property]):
        res = Response.get(
            self._db._turing,
            "ListEntityProperties",
            db_id=self._db.id,
            entity_type=self._entity_type,
            entity_id=self.id,
        )
        if res.failed():
            return res
        res.data = {p.property_type_name: Property(p) for p in res.data.properties}
        return res

    def add_property(self, pt: PropertyType, value) -> Response[Property]:
        if pt.value_type == APIService_pb2.ValueType.INT:
            oneof = "int64"
        elif pt.value_type == APIService_pb2.ValueType.UNSIGNED:
            oneof = "uint64"
        elif pt.value_type == APIService_pb2.ValueType.STRING:
            oneof = "string"
        elif pt.value_type == APIService_pb2.ValueType.BOOL:
            oneof = "bool"
        elif pt.value_type == APIService_pb2.ValueType.DECIMAL:
            oneof = "decimal"

        res = Response.get(
            self._db._turing,
            "AddEntityProperty",
            db_id=self._db.id,
            entity_id=self.id,
            entity_type=self._entity_type,
            property=APIService_pb2.Property(
                property_type_name=pt.name, value_type=pt.value_type, **{oneof: value}
            ),
        )
        if res.failed():
            return res

        res.data = Property(res.data.property)
        return res


class Node(Entity):
    def __init__(self, net: Network, node):
        super().__init__(node.id, net._db, APIService_pb2.EntityType.NODE)
        self._net = net
        self._name = node.name
        self._in_edges_ids = node.in_edge_ids
        self._out_edges_ids = node.out_edge_ids


class Edge(Entity):
    def __init__(self, db: Database, edge):
        super().__init__(edge.id, db, APIService_pb2.EntityType.EDGE)
        self._source_id = edge.source_id
        self._target_id = edge.target_id
