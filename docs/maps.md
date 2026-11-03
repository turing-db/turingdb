We wish to support the CYPHER map type as a stored property value; https://neo4j.com/docs/cypher-manual/current/values-and-types/maps/.

> NOTE: Neo4j does not store maps as properties. Here a stored map may hold scalars, nulls,
> embeddings, lists and nested maps, and a stored list may hold maps.

# Query-scoped maps

`storage/map/` mirrors `storage/list/`. `MapBuffer` stores entries contiguously and hands out a
`MapView`: a span over `MapEntryView`s. An entry is laid out as a 16-byte `std::string_view` key,
a one-byte `CellTag`, then the bytes of the value.

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

A map entry is a key and a cell: `MapEntryView::getValue()` views the `[tag][value]` bytes that
follow the key, which are laid out as a list element's are. The result of a key read is a
`!storage.any` chunk, `ColumnVector<CellView>`, one cell per row, viewing the value the map
already holds, so the read copies nothing. A row whose map holds no such key, and a null row, read
`CellView::nullElement()`. The null therefore lives in the cell's own tag, and the column is
never wrapped in a `nullable`.

From there a map value is any other value of type any: the binary protocol writes it as a
`CELL_VIEW` column, the renderers print it as a cell, and `IS NULL`, `=`, `<>`, the
orderings, `IN`, `keys()`, `properties()` and the index read it through the `CellView`
kernels.

So `WHERE n.attrs.x = 1` drops the rows whose map holds no `x`, and `WHERE NOT (n.attrs.x = 1)`
drops them too.

Beyond that and `keys()` / `properties()` below, a map value is passed through and rendered.
Ordering by one, deduplicating on one, doing arithmetic on one and writing one back with `SET` are
all rejected, `ORDER BY` and `DISTINCT` by the chunk-kind selectors rather than by the analyzer,
so they read as `Unsupported chunk element type`. So is unwinding one: `UNWIND m.l AS x` is turned
away with `UNWIND requires a list, not 'MapValue'`, whatever the entry's tag holds.

The bracket form reads a key too. Where `m` is a map, a node, a relationship or a loaded row,
`m['key']` is analyzed as its dotted form `m.key` (`IndexExpr::getDottedForm`), and grouping and
ordering match the two as one expression: `WITH {a: 1} AS m RETURN m['a']` is 1, and `p['age']` is
the integer `p.age` is. On a tagged cell, `m['key']` is emitted as a `db.dynamic_map_key` over the
key as a constant, the op `m[k]` emits for a key computed per row. That read raises on a datetime
or a duration, where `m.key` reads the component.

# Reading through a nested value

`m.a.b` reads `b` out of the value `m.a` holds, as `n.attrs.b` reads it out of a stored map. The
parser reads a dotted chain as Neo4j does: a `PropertyExpr` holds a variable and one name, and each
further name is a `PropertyLookupExpr` over the value before it, so `m.a.b.c` is `((m.a).b).c`, the
tree its parenthesised spelling builds. `WITH {d: {e: 1}} AS m RETURN m.d.e` is 1, and
`MATCH (n:Person) RETURN n.nosuch.x` is null on every row, as a key of null is.

A map value is a tagged cell, so a read through one asks the row's tag rather than the column's
type. `m[k].b` reads `b` out of the map `m[k]` holds, and `m.l[0]` reads the first element of
the list `m.l` holds. Where the tag says a node or an edge the key read answers its property,
and where it says a datetime or a duration the dotted form answers the component the key names,
as Neo4j does: `WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d.year`
is 2020. Any other value has no key, and the key read raises on it, as `d[k]` does on a
datetime or a duration. Where the tag says no list the index raises too. A null reads null.

A key read is speculatable only where its operand provably holds maps, nodes, edges and nulls
alone: a column of one of those types, a merge of maps, an unwound literal list of maps, a
selection between such columns, or one of them carried through a filter, a cross product or a
hash join. So `row.src` in `UNWIND [{src: 'Remy'}, {src: 7}] AS row MATCH (a {name: row.src})`
can still be sunk into its factor as a hash join key.

A key read takes a cell column wherever it takes a map column: `holdsAMapPerRow` says so in
`DBLowering` and in the two nl verifiers, and `NLExecutor::selectMapRead` takes the `MapView`
out of the tag. An index takes a cell column the same way, and the result is the
`nullable<any>` every index gives.

A map held in a list reads through those same two paths, since `xs[0]` hands back a
`!storage.any` whose `MapView` tag is the map. An `UNWIND` of such a list binds its
variable to the same cell, so `UNWIND [{a: 1}] AS m RETURN m.a` reads through one too. The
dotted form `xs[0].a` needs a parser rule of its own (`propertyLookupExpr`), because a `.`
after a `]` is otherwise no expression.

## Maps inside stored lists

A `ListContainer` keeps the map elements of its lists in a `MapContainer` of its own, created on
first use (`ListContainer::getMaps`). A `MapContainer` holds its list values in a `ListContainer`
by value, so the two types nest, but the objects form a tree. `EncodedList` encodes a map element
as `[u64 length][EncodedMap bytes]`.

# Map projection

`v {.name, .*, rank: v.age + 1, x}` builds a map out of `v`, a node, a relationship or a map.
`.key` holds `v.key`, null where `v` has no such property, and `.*` every property of `v`.
`key: expr` holds the expression, and `x` the variable `x` under the key `x`. Every entry is
computed, and a later one replaces an earlier one under the same key, as in a map literal:
`v {x: 1 / 0, x: 2}` raises. An entry wins over the property `.*` reads under its key wherever
the `.*` stands, as in Neo4j: `p {name: 'x', .*}` and `p {.*, name: 'x'}` both hold the name
'x'. A null `v` projects to null and computes none of its items.

The AST keeps the projection as a `MapProjectionExpr`; the analyzer reads each `.key` as the
`v.key` property read. Codegen emits a `db.make_map` of the entries and, where the projection
reads every property, a `db.merge_maps` of it over `db.properties` of `v`. The map is built in
a `db.lazy_case` on `v IS NOT NULL`, so only the rows where `v` is not null compute it.

`db.merge_maps` lowers one-for-one to `nl.merge_maps`. Row r holds every key the operands hold
at row r, under the value of the last operand holding it, sorted on key. A row where an operand
holds no map gets no map.

`(n {key: value})` is also a node pattern. Where a pattern may start, as in
`WHERE (n {name: 'x'})-->()`, the projection is read as one: its entries become the node's
properties. Any other item makes it no node pattern.

`collect()` gathers maps as it gathers lists. A null map is dropped, and `collect(DISTINCT m)`
compares maps entry by entry.

# keys() and properties()

`keys(x)` returns the property names of a node or a relationship, or the keys of a map, sorted.
`properties(x)` returns a node's or a relationship's properties as a map, and a map unchanged.
Both also read a list element or a map value holding a node, a relationship or a map, and
return null for a null.

An entity's properties are read across every datapart, newest first, so `n.age` set in a later
commit appears once with its newest value, and one a later commit removed does not appear. The
query's own writes are included: `MATCH (n) SET n.x = 1 RETURN keys(n)` lists `x`, and
`CREATE (n:T {a: 1}) RETURN properties(n)` returns `{a: 1}`. A value the query wrote reads back
as the type the schema holds the property as.
