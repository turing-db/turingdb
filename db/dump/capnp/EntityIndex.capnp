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
    nodeTypeNameId @2 :UInt64;
    properties @3 :List(Property);
}

struct Edge {
    id @0 :UInt64;
    edgeTypeNameId @1 :UInt64;
    sourceId @2 :UInt64;
    targetId @3 :UInt64;
    properties @4 :List(Property);
}

struct Network {
    nameId @0 :UInt64;
    nodes @1 :List(Node);
    edges @2 :List(Edge);
}

struct EntityIndex {
    networks @0 :List(Network);
}
