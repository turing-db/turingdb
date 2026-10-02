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

# Reading one key

`m.key` reads one value out of a map, for a key spelled in the query. It is a variable bound
to a map (`WITH {a: 1} AS m RETURN m.a`), a stored map property (`n.attrs.key`) or the map an
expression returns (`startNode(r).attrs.key`). `db.map_key` carries the key as a `StrAttr` and
lowers one-for-one to `nl.map_key`.

The result is a `!storage.map_element` chunk: `ColumnVector<MapEntryView>`, one entry per row,
viewing the entry the map already holds, so the read copies nothing. A row whose map holds no
such key, and a row holding no map, read an entry under that same key tagged `Null`, built once
per statement and shared by all of them - so every row of the column names the key it was read
under. The null therefore lives in the entry's own tag, and the column is never wrapped in a
`nullable`.

The binary protocol writes the column as `MAP_ENTRY_VIEW`: `[mapByteSize]` then one
`[keyLen][key][tag][value]` entry per row, which is what a map's own entries are. The column is
therefore one map of as many entries as it has rows, and both sides reuse the map path -
`writeMapView` without its header going out, `beginMap` and the ordinary drain coming back.
Every row repeats the key, since the encoder sees only columns and the schema is declared
before any row exists; TUR-112 tracks giving a column type somewhere to say such a thing once.

Renderers print the value alone, not the pair: a cell of the column is the value the key named.
`JsonEncoder::encodeValue(MapEntryView)`, `TuringShell::asString` and
`QueryResultFormatter::valueToString` write the value, and their `encodeEntry` /
`entryAsString` / `entryToString` siblings are what add the key for an entry read inside its
map.

`IS (NOT) NULL` reads the entry's tag, the way it reads a list element's: a key the map does
not hold, a row holding no map and a key whose stored value is null all answer alike. That is
what `operator==(MapEntryView, PropertyNull)` in `storage/list/ListElementOrder.h` is for, and
why it has to be declared - `PropertyNull.h`'s fallback would answer false for a type it knows
nothing about.

`=` and `<>` compare an entry against a value of a known type, and against another entry. The
entry holds its own type, so it is equal only to a value of that type holding the same thing:
`n.attrs.x = '1'` is false where `x` holds `1`. The kernels are the `MapEntryView` overloads of
`TuringEqual` and `TuringNotEqual`, admitted by `MapEntryKindPairs` in `AllowedKinds.h`.

They return `std::optional<CustomBool>`, because an entry tagged `Null` answers unknown rather
than false. `binaryResultElement` has to declare the result `nullable<i1>` to match, which is
what `comparesAMapValue` is for: a map value's null is in its tag, so comparing one is
three-valued whatever the column's shape. `IS (NOT) NULL` is excluded from that rule, since its
kernel answers about the tag rather than being made unknown by it, and so stays two-valued. The
two decisions are made independently and nothing checks them against each other - TUR-135.

So `WHERE n.attrs.x = 1` drops the rows whose map holds no `x`, and `WHERE NOT (n.attrs.x = 1)`
drops them too.

Beyond that a map value is passed through and rendered, nothing more. Ordering by one,
deduplicating on one, doing arithmetic on one and writing one back with `SET` are all rejected,
`ORDER BY` and `DISTINCT` by the chunk-kind selectors rather than by the analyzer, so they read
as `Unsupported chunk element type`. So is `m.a.b` - a key of a map held by another map - and so
is unwinding one, which leaves `UNWIND [{a: 1}] AS m RETURN m.a` and a key holding a list out of
reach for now.

## Maps inside stored lists

A `ListContainer` keeps the map elements of its lists in a `MapContainer` of its own, created on
first use (`ListContainer::getMaps`). A `MapContainer` holds its list values in a `ListContainer`
by value, so the two types nest, but the objects form a tree. `EncodedList` encodes a map element
as `[u64 length][EncodedMap bytes]`.
