We wish to support the CYPHER list type; https://neo4j.com/docs/cypher-manual/current/values-and-types/lists/.

# Theory

Lists can be heterogeneous; storing multiple different types in the same list.


Lists would interact with the `UNWIND` keyword, expanding each value as input to subsequent statements.


To begin with, we will not support lists as property values, but only as ephemeral objects in a query.
> NOTE: Neo4j only supports homogeneous lists as properties.


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


Should we wish to support list properties in the future, the list property container could act as an
equivalent "list buffer" for the non-ephemeral case.


Since a list buffer may have views into it, the data must be stable. Since we may need to add new lists to
an existing buffer, it also need be dynamically sized. We cannot use `std::vector` as it does not guarantee
pointer stability.

# Implementation

`storage/ListBuffer.h` implements the required datastructure. `ListBuffer` provides stable references to
contiguously stored list elements, as well as views into each element: `ListElementView`.


Each list element is stored packed and type erased: a "type tag" (`ListBufferTypeTag`) followed by the bytes
of the value of the element. Each element must be trivially copyable.


A `ListElementView` is a slim wrapper over a pointer to the element's type tag. Helper functions are provided
to interpret the tag. The type tag is used to determine the number of bytes of the value of the element, which
can then be read into a generic `T` with the `getAs` helper function.


Since the `ListBuffer` guarantees that all elements in a single list are stored contiguously, a `ListView` is
a span over the `ListElementViews` which make up the elements of the list.


We can then have `using types::List::Primitive = ListView`, with the desired properties outlined above.
