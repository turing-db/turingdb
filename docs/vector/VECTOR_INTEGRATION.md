# Vector DB integration in TuringDB

We want to integrate in the query language the vector DB implementation in the vector/ directory and exemplified by the sample program in samples/vector-db directory.

## Query commands

The query commands that we want can be possibly decomposed into several steps:

1. Create a vector index for a graph inside TuringDB.
This is independent of versioning.

CREATE VECTOR INDEX vectordb

2. Load embeddings vectors inside a vector index from a file. A numerical ID is associated to each embedding vector
This writes the vector index on disk.

LOAD EMBEDDINGS FROM "myfilepath" IN vectordb

4. Get a column of the numerical values associated to the n nearest vectors for a given query vector

VECTOR SEARCH vectordb "embedding vector"

Returns a column of IDs associated to the k neareast vector for a given query vector given for now as a string.
Later: maybe as a list?

Alternative 2: if we used a procedure with the CALL..YIELD syntax we could choose if we wanted to return just the IDs
or also the associated vectors as well.

5. Delete vector index

DELETE VECTOR INDEX vectordb

Concurrency: the vector index would need to be properly guarded for concurrent access.

## Load and save

When we load a graph we will need to load as well any vector index that may have been created in this graph.

Vector index folder: in the directory storing a graph, we could have a folder with a well-known name indicated the presence of a vector index to load.
The loading and dumping itself is already implemented.
