@0xcfe35582b98fec68;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("db::OnDisk");

struct PropertyMap {
    entries @0 :List(Entry);
    struct Entry {
        nameId @0 :Text;
        type @1 :PropertyType;
    }
}

enum ValueKind {
    invalid @0;
    int @1;
    unsigned @2;
    bool @3;
    decimal @4;
    stringRef @5;
    string @6;
    enumSize @7;
}

struct PropertyType {
    id @0 :UInt64;
    nameId @1 :UInt64;
    kind @2 :ValueKind;
}

struct NodeType {
    nameId @0 :UInt64;
    propertyTypes @1 :List(PropertyType);
}

struct EdgeType {
    nameId @0 :UInt64;
    propertyTypes @1 :List(PropertyType);
    sources @2 :List(UInt64);
    targets @3 :List(UInt64);
}

struct TypeIndex {
    edgeTypes @0 :List(EdgeType);
    nodeTypes @1 :List(NodeType);
}
