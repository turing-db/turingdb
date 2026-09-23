We wish to support the CYPHER map type as a stored property value; https://neo4j.com/docs/cypher-manual/current/values-and-types/maps/.

> NOTE: Neo4j does not store maps as properties. Here a stored map may hold scalars, nulls,
> embeddings, lists and nested maps, and a stored list may hold maps.

# Query-scoped maps

`storage/map/` mirrors `storage/list/`. `MapBuffer` stores entries contiguously and hands out a
`MapView`: a span over `MapEntryView`s. An entry is laid out as a 16-byte `std::string_view` key,
a one-byte `MapBufferTypeTag`, then the bytes of the value.

The key is a view. So is a string or embedding value, and a list or nested map value is a view
into another buffer. A `MapBuffer` therefore owns none of the data a map refers to.

# Map properties

`ValueType::Map` is a stored property type, held by `TypedPropertyContainer<types::Map>`: one
`MapView` per entity, over entries the container owns.

`storage/map/MapContainer.h` owns those entries, as `ListContainer` does for lists. Everything
entering it is deep-copied: keys and string payloads into its `StringBuffer`, embeddings into its
float `SpanBuffer`, list values into its own `ListContainer`, nested maps recursively.

Entries are stored sorted by key. Codegen already emits map literals with sorted keys; the
container sorts again so the invariant holds for every producer. Two equal maps then hold their
entries in the same order, so equality and hashing (`storage/map/MapHash.h`) walk both maps
entry by entry.

A map that has to outlive the buffers that built it travels as an `EncodedMap`
(`storage/map/EncodedMap.h`). The format is `[u64 count]`, then per entry
`[u64 keyLength][key][u8 tag][payload]`. Payloads follow `EncodedList`'s rules. A list value is
`[u64 length][EncodedList bytes]` and a nested map is encoded recursively. The write buffer
stages a `CREATE` / `SET` value in this form until the commit builds its datapart, and the
dumper writes the same encoding to disk.

## Maps inside stored lists

A `ListContainer` keeps the map elements of its lists in a `MapContainer` of its own, created on
first use (`ListContainer::getMaps`). A `MapContainer` holds its list values in a `ListContainer`
by value, so the two types nest, but the objects form a tree. `EncodedList` encodes a map element
as `[u64 length][EncodedMap bytes]`.
