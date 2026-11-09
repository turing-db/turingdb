We wish to support the CYPHER list type; https://neo4j.com/docs/cypher-manual/current/values-and-types/lists/.

# Theory

Lists can be heterogeneous; storing multiple different types in the same list.


Lists would interact with the `UNWIND` keyword, expanding each value as input to subsequent statements.


Lists are supported both as ephemeral objects in a query and as stored property values.
> NOTE: Neo4j only supports homogeneous lists as properties; a stored list here may be heterogeneous
> and may hold nested lists.


An approach that would support ephemeral query-scoped lists as well as enduring property lists, involves serving
`ColumnVector<List::Primitive>` as a viewing span onto backed memory which stores the list elements.


A query-scoped "list buffer" could be a contiguous block of elements of all lists.


This ensures:

1. for a given list, all its elements are stored contiguously in memory
2. for lists a and b which are allocated (entered into the buffer) one after another,
   the elements of a and the elements of b are contiguous with respect to each other


This allows us to keep a slim interface on columns, whilst maintaining good locality of both intra- and inter-
elements of lists.


Moreover, since a column of lists is merely a vector of spans, operations which involve copying rows (e.g.
Cartesian Product, Joins, etc.) have constant time complexities - as opposed to storing list data directly
in the columns, which would scale with the size of the lists.


`ListContainer` is the non-ephemeral counterpart of the query-scoped list buffer, owning the lists a
datapart's property container holds.


Since a list buffer may have views into it, the data must be stable. Since we may need to add new lists to
an existing buffer, it also need be dynamically sized. We cannot use `std::vector` as it does not guarantee
pointer stability.

# Implementation

`storage/ListBuffer.h` implements the required datastructure. `ListBuffer` provides stable references to
contiguously stored list elements, as well as views into each element: `CellView`.


Each list element is stored packed and type erased: a "type tag" (`CellTag`) followed by the bytes
of the value of the element. Each element must be trivially copyable.


A `CellView` is a slim wrapper over a pointer to the element's type tag. Helper functions are provided
to interpret the tag. The type tag is used to determine the number of bytes of the value of the element, which
can then be read into a generic `T` with the `getAs` helper function.


Since the `ListBuffer` guarantees that all elements in a single list are stored contiguously, a `ListView` is
a span over the `CellViews` which make up the elements of the list.


We can then have `using types::List::Primitive = ListView`, with the desired properties outlined above.

## List properties

`ValueType::List` is a stored property type, held by `TypedPropertyContainer<types::List>` the way a string
is held by `TypedPropertyContainer<types::String>`: one `ListView` per entity, over elements the container
owns.


`storage/list/ListContainer.h` owns those elements. A `ListBuffer` alone is not enough: a string or embedding
element holds only a view of its payload, and a list handed over by a query points into buffers that query
frees. So everything entering a `ListContainer` is deep-copied - string payloads into its `StringBuffer`,
embedding payloads into its float `SpanBuffer`, nested lists recursively - and the views it hands out stay
valid for as long as it does.


A view cannot leave the buffers that built it, so a list that has to outlive them travels as an `EncodedList`
(`storage/list/EncodedList.h`): a self-describing byte sequence carrying the whole value, nested lists and
payloads included. It is the owning form the write buffer stages a `CREATE` / `SET` value in until the commit
builds its datapart, and the same encoding the dumper writes to disk and the loader reads back.

## Paths

A list element or a map value may be a path, tagged `CellTag::Path`. Its value is a `PathView`
(`storage/PathView.h`): a span over the path's IDs, node, edge, node, ... as `GraphPath` lays
them out, held in a buffer beside the elements. The query buffers view the IDs of the path
column they read from, and `ListContainer`, `MapContainer` and `NLValueStore` copy them into a
`SpanBuffer<uint64_t, PathView>` of their own, the way they copy an embedding's floats.

A path is a value of the query that matched it, so a property cannot hold one: a write of a list
or a map holding a path, at any depth, is rejected. `EncodedList` and `EncodedMap` still encode a
path, as its ID count then its IDs, since they also compare query values.

## Schema IDs

A list element or a map value may be the ID of a label, an edge type or a property type, or a
value type: what `db.labels()`, `db.edgeTypes()` and `db.propertyTypes()` yield. Each has a tag of
its own, `CellTag::LabelID`, `EdgeTypeID`, `PropertyTypeID` and `ValueType`, and is laid out as a
node ID is: the tag, then the fixed-width value.

An element equals a value of its own kind holding the same number, and nothing else: label 3 is
not edge type 3, nor the integer 3. A list holding schema IDs sorts as their columns sort, an ID by
its number and a value type by its ordinal, and the four kinds sort after every number and before
null. `<` and its siblings order none of them.

A schema ID is a value of the query, as a path is, so a property cannot hold one: a write of a list
or a map holding one, at any depth, is rejected. `EncodedList` and `EncodedMap` encode it as its
fixed-width value, since they also compare query values.

