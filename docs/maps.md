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
or a duration, where `m.key` reads the component. A map literal is read by a key computed per row
the same way: `UNWIND ['a', 'b', 'c'] AS k RETURN {a: 1, b: 'x'}[k]` is 1, x and null.

A node or a relationship is read by a key computed per row the same way, openCypher's dynamic
property access: `UNWIND ['name', 'age'] AS k MATCH (n:Person {name: 'Remy'}) RETURN n[k]` is
Remy and 32. `db.dynamic_map_key` reads the node column itself, with no cell in between. A null
key, a property the node lacks and a node an OPTIONAL MATCH missed read null. The key is a
string: `n[0]` is rejected with `A node is indexed by a string key, not by 'Integer'`, and a
tagged cell holding 0 raises the same.

A key holding one string in every row reads the property as the dotted form does, where the graph
types that name. `fold_constant_map_keys` reads `MATCH (n:Person) WITH n, 'name' AS k RETURN n[k]`
with `db.get_node_properties(%n, "name")`, and `db.to_any` hands the typed column to the readers
of the tagged cell the read by name made. A comparison with a value of the same kind, two numbers
or two strings, booleans, datetimes or durations, compares the typed column itself, so
`WHERE n[k] = 'Remy'` plans the `db.scan_nodes_by_property_value` that `WHERE n.name = 'Remy'`
plans. A name the graph does not type, one only the query creates included, stays a read by name.

An index held in a tagged cell can be a position on one row and a key on the next, so `c[i]` with
such an `i` emits `db.container_index` where `c` is a list, a map, a node, a relationship or a
tagged cell. Neo4j's ContainerIndex expression reads it the same way. A row whose base holds a list
and whose index holds an integer reads the element, as `db.list_index` does. A row whose base holds
a map, a node or an edge and whose index holds a string reads the value under that key, as
`db.dynamic_map_key` does. A null base or a null index reads null, and any other pair raises:
`UNWIND [1, 'a'] AS b RETURN [1, 2, 3][b]` raises on the row holding `'a'`. The error names the
index the base takes and the type the row held, as the analyzer words it: `A list index expression
must be an integer, not 'String'` there, and `A map is indexed by a string key, not by 'Integer'`
for `UNWIND [1, 'k'] AS b RETURN {k: 1}[b]`. A base holding 1 raises `Index operator [] can only be
applied to a list, a map, a node or a relationship, not 'Integer'`. An integer, a string or a null
index keeps the op it emitted before.

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
alone, and an index or a slice only where its operand holds lists and nulls alone.
`holdsOnlyNullOr` (`DBHeldTags`) walks the operand back to where its values were made: through
every op implementing `ColumnCarrierOpInterface` (a filter, a sort, a limit, a hop, an unwind's
carry set, a factor, an OPTIONAL MATCH, a UNION branch, a CASE), and from a key or element read
to the literal, `db.make_map` or `db.make_list` it reads. So `row.s.name` in
`UNWIND [{s: {name: 'Remy'}}, {s: null}] AS row MATCH (a {name: row.s.name})` can still be sunk
into its factor as a hash join key, and so can `row.src` after `WITH row ORDER BY row.src`.

A key read takes a cell column wherever it takes a map column, and a column of nodes or edges
too: `holdsAKeyedValuePerRow` says so in `DBLowering` and in the nl verifiers, and
`NLExecutor::selectKeyedRead` takes the `MapView`, the node or the edge out of the tag, or the ID
out of the entity column. An index takes a cell column the same way, and the result is the
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

`v {.name, .*, rank: v.age + 1, x}` builds a map out of `v`, a node, a relationship, a map, a
datetime, a duration or a point. `.key` holds `v.key`, null where `v` has no such property, and
`.*` every property of `v`. On a datetime, a duration or a point `.key` holds the component
`v.key` reads: `WITH duration({hours: 1}) AS d RETURN d {.hours, x: 1}` is `{hours: 1, x: 1}`.
These values have no properties, so `.*` and a name that is no component are rejected by the
analyzer where `v` is typed one of them, and raise on the row where a tagged cell holds one.
`key: expr` holds the expression, and `x` the variable `x` under the key `x`. A later entry
shadows an earlier one under the same key, as in a map literal: `v {x: 1 / 0, x: 2}` is
`{x: 2}`, and `v {.name, name: 1 / 0}` raises. An entry wins over the property `.*` reads under
its key wherever the `.*` stands, as in Neo4j: `p {name: 'x', .*}` and `p {.*, name: 'x'}` both
hold the name 'x'. `.*` shadows no entry, so `p {name: 1 / 0, .*}` raises. A null `v` projects
to null and computes none of its items.

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

# A repeated key

A map written with a repeated key holds the last entry under it and never computes an earlier
one, as Neo4j does: `RETURN {x: 1 / 0, x: 2}` is `{x: 2}`. The earlier entry is still analyzed.
`RETURN {x: y, x: 2}` is an analysis error where `y` is not defined, and
`UNWIND [1, 2] AS i RETURN {x: count(i), x: 2}` aggregates to one row. `MapLiteral` keeps every
entry it was written with, and `getHeldEntries()` the last one under each key, which is all
codegen translates. A map projection marks its shadowed items (`MapProjectionExpr::Item`'s
`_shadowed`).

A pattern's property map is read by its clause. MATCH and OPTIONAL MATCH read every entry as an
equality, and so do the patterns of EXISTS, COUNT and COLLECT subqueries, pattern predicates and
pattern comprehensions: `MATCH (n {name: 'Adam', name: 'Remy'})` matches nothing, and
`MATCH (n:Person {age: 1 / 0, age: 32})` raises. CREATE and MERGE write the held entries alone:
`CREATE (n:P {a: 1 / 0, a: 2})` writes 2, and `MERGE (n:P {a: 1, a: 2})` matches and creates on
`a = 2`.

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
