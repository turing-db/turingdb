# Vector Query Language Syntax

This document provides a quick reference for the vector search commands in TuringDB.

## Commands Overview

| Command | Description |
|---------|-------------|
| `CREATE VECTOR INDEX` | Create a new vector index |
| `LOAD VECTOR` | Load embeddings from a file |
| `VECTOR SEARCH` | Find k-nearest neighbors |
| `DELETE VECTOR INDEX` | Delete a vector index |
| `SHOW VECTOR INDEXES` | List all vector indexes |

---

## CREATE VECTOR INDEX

Creates a new vector index with a specified dimension and distance metric.

**Syntax:**
```
CREATE VECTOR INDEX <name> WITH DIMENSION <dim> METRIC <metric>
```

**Parameters:**
- `<name>` - Identifier for the vector index
- `<dim>` - Dimension of the embedding vectors (positive integer)
- `<metric>` - Distance metric: `EUCLID` (Euclidean distance) or `COSINE` (cosine similarity)

**Examples:**
```cypher
-- Create a 128-dimensional index with Euclidean distance
CREATE VECTOR INDEX embeddings WITH DIMENSION 128 METRIC EUCLID

-- Create a 768-dimensional index for BERT embeddings with cosine similarity
CREATE VECTOR INDEX bert_vectors WITH DIMENSION 768 METRIC COSINE

-- Create a small 4-dimensional index for testing
CREATE VECTOR INDEX test_index WITH DIMENSION 4 METRIC EUCLID
```

---

## LOAD VECTOR

Loads embedding vectors from a file into an existing vector index. Each vector is associated with a numerical ID.

**Syntax:**
```
LOAD VECTOR FROM "<filepath>" IN <index_name>
```

**Parameters:**
- `<filepath>` - Path to the file containing embeddings (string literal)
- `<index_name>` - Name of the target vector index

**Examples:**
```cypher
-- Load embeddings from a CSV file
LOAD VECTOR FROM "/data/embeddings.csv" IN embeddings

-- Load BERT vectors from a specific path
LOAD VECTOR FROM "/models/bert/document_vectors.csv" IN bert_vectors

-- Load test data
LOAD VECTOR FROM "./test_data/vectors.csv" IN test_index
```

---

## VECTOR SEARCH

Searches for the k nearest neighbors of a query vector in the specified index. This is a read statement that can be combined with MATCH clauses.

**Syntax:**
```
VECTOR SEARCH IN <index_name> FOR <k> (<vector>) YIELD <variable> [, score]
```

**Parameters:**
- `<index_name>` - Name of the vector index to search
- `<k>` - Number of nearest neighbors to return (positive integer)
- `<vector>` - Query vector as a parenthesised list of float values
- `<variable>` - Variable name to hold the result nodes (typically `ids`)
- `score` - Optional second yielded variable, the distance each neighbor scored

`ids` is the node the index holds each vector under, so a pattern can walk out of it
directly and an equality can compare a matched node to it. The query vector must have
as many elements as the index' dimension.

**Examples:**
```cypher
-- Find 10 nearest neighbors for a 4-dimensional query vector
VECTOR SEARCH IN test_index FOR 10 (0.256, 0.12, 0.12345, 0.89) YIELD ids RETURN ids

-- Report the distance each neighbor scored alongside it
VECTOR SEARCH IN test_index FOR 10 (0.256, 0.12, 0.12345, 0.89) YIELD ids, score
RETURN ids, score

-- Walk out of the neighbors: the pattern names the yielded variable itself
VECTOR SEARCH IN embeddings FOR 10 (0.5, 0.3, 0.8, 0.1) YIELD ids
MATCH (ids)-[:CITES]->(m)
RETURN ids, m.title

-- The same traversal written the other way round
VECTOR SEARCH IN embeddings FOR 10 (0.5, 0.3, 0.8, 0.1) YIELD ids
MATCH (n:Document)-[:CITES]->(m) WHERE n = ids
RETURN n.title, m.title
```

---

## DELETE VECTOR INDEX

Deletes an existing vector index and frees associated resources.

**Syntax:**
```
DELETE VECTOR INDEX <name>
```

**Parameters:**
- `<name>` - Name of the vector index to delete

**Examples:**
```cypher
-- Delete the test index
DELETE VECTOR INDEX test_index

-- Delete the embeddings index
DELETE VECTOR INDEX embeddings

-- Clean up BERT vectors
DELETE VECTOR INDEX bert_vectors
```

---

## SHOW VECTOR INDEXES

Lists all vector indexes in the database with their metadata.

**Syntax:**
```
SHOW VECTOR INDEXES
```

**Examples:**
```cypher
-- List all vector indexes
SHOW VECTOR INDEXES
```

---

## Complete Workflow Example

```cypher
-- 1. Create a vector index for document embeddings
CREATE VECTOR INDEX doc_embeddings WITH DIMENSION 384 METRIC COSINE

-- 2. Load pre-computed embeddings from a file
LOAD VECTOR FROM "/data/document_embeddings.csv" IN doc_embeddings

-- 3. Search for similar documents given a query embedding
VECTOR SEARCH IN doc_embeddings FOR 5 (0.12, 0.45, 0.78, ...) YIELD ids, score
MATCH (d:Document) WHERE d = ids
RETURN d.title, d.summary, score

-- 4. View all indexes
SHOW VECTOR INDEXES

-- 5. Clean up when done
DELETE VECTOR INDEX doc_embeddings
```
