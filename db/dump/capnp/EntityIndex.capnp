@0xb04dff74e804e796;

using Cxx = import "/capnp/c++.capnp";
using TypeIndex = import "TypeIndex.capnp";
$Cxx.namespace("db::OnDisk");

struct Property {
    kind @0 :TypeIndex.ValueKind;
    propertyTypeNameId @1 :UInt64;
    value :union {
        int @2: Int64;
        unsigned @3: UInt64;
        bool @4: Bool;
        decimal @5: Float64;
        string @6: Text;
        stringRefId @7: UInt64;
    }
}

struct Node {
    id @0 :UInt64;
    nameId @1 :UInt64;
    networkId @2 :UInt64;
    nodeTypeNameId @3 :UInt64;
    properties @4 :List(Property);
}

struct Edge {
    id @0 :UInt64;
    networkId @1 :UInt64;
    edgeTypeNameId @2 :UInt64;
    sourceId @3 :UInt64;
    targetId @4 :UInt64;
    properties @5 :List(Property);
}

struct NodeSpan {
    nodes @0 :List(Node);
}

struct EdgeSpan {
    edges @0 :List(Edge);
}

struct Network {
    nameId @0 :UInt64;
}

struct EntityIndex {
    networks @0 :List(Network);
    nodeSpans @1 :List(NodeSpan);
    edgeSpans @2 :List(EdgeSpan);
}
