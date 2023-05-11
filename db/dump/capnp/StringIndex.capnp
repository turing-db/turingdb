@0xf3caca6c993d9e80;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("db::OnDisk");

struct SharedString {
    id @0 :UInt64;
    str @1 :Text;
}

struct StringIndex {
    strings @0 :List(SharedString);
}
