# Open Questions
- How do we define the interface of a generic index?
- How/when do we decide whether to consult an index or a normal `GetPropertiesIterator`? plan time? execution time?

# Indexes

We wish to have *indexes* which speed up retrieval of certain data
from the DB. An example of this is a *property indexer* which allows
for faster retrieval of all the nodes with a certain property value $x$.
The query
```
MATCH (n) WHERE n.property = x RETURN n
```
would then be able to use a single "index lookup" step, rather than a
combination of `ScanNodes` followed by a `Filter` based on property values.

Below we may refer to "index on a property" for examples, but the idea remains
the same for any index.

# Implementation

Indexes should be integrated into the version control system in the DB.

Indexes should be optionally-creatable on pending (uncommitted) and frozen
(committed) data parts.

Since dataparts are immutable, indexes cannot live inside of a datapart, as
it would then be impossible to create an index on an existing datapart without
creating a copy of all data in that part, along with an index.

Just as multiple commits share dataparts, multiple commits should be able to share
indexes on those dataparts. This means that we require a similar reference-sharing
model of indexes that we have for dataparts. This motivates the `IndexManager` class.

## `IndexManager`

The `IndexManager` class should be the true owner of all index objects for a graph.
Just as `Commit`s store references to the dataparts which are visible to them, they
should store references to indexes that concern them.

A given index may be used to index the data contained within one or more dataparts.

>[!note] Required Operation
Determine whether there exists an index on property `Y` visible to the current commit.

### Immutability of Indexes

To preserve lock-free, indexes will be immutable just as dataparts are.

The immutability allows multiple commits to hold references to the same index, and
read them without a lock, since they will never change.

## Extending Indexes
Consider the flow:

1. `CHANGE NEW; CHECKOUT CHANGE-x`
2. Create a number of nodes; with property `Y`
3. `CREATE NODE INDEX ON Y`
4. `CHANGE SUBMIT`

5. `CHANGE NEW; CHECKOUT CHANGE-x+1`
6. Create a number of new nodes; with property `Y`
7. `CHANGE SUBMIT`

At stage 7, there will be a number of new nodes which would've been indexed if they
were visible at 3., but will not be present in that index.

To reduce complications in cases where a commit views some dataparts with an index on
a property, and views other dataparts where no such index exists, we use the
`CREATE INDEX` command to say:

"create an index on the specified property, and keep it up to date until I say so";

where the user can exit this contract with the `DROP INDEX` command.

Subsequently, a `CHANGE SUBMIT` requires a check for the existence of any currently-
tracked indexes, so that they may be extended as per the above contract.

Step 7. then creates an additional index on property `Y`, but only for the property
values stored in the nodes created at 6.

That is, if $I$ is the set of property values indexed by index created at 3., and $J$
is the set of property values indexed by the index created at 7., then $I \cap J = \emptyset$.

This means we do not need to reindex all that is indexed by $I$, but means that we need
to linear search through indexes to find all matches. This is in line with our current
philosophy as we already consider it bad practices to split nodes and edges over many
dataparts/commits.

# Serialisation
All indexes need to be able to dumped to disk and loaded.

Indexes should be dumped "flat" just as dataparts, so that an arbitrary commit can be
loaded, along with all of the indexes it requires.

# Summary of operations
