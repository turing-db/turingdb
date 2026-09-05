#ifndef TURINGDB_SYSTEM_OPS
#define TURINGDB_SYSTEM_OPS

include "DBDialect.td"
include "DBTypes.td"
include "StorageEnums.td"

// Base of the system-command family: the statements that act on the server
// rather than on the graph's rows - loading a graph, managing changes,
// transferring files, declaring indexes.
//
// They share a shape no dataflow op has. None takes a column operand: every
// parameter is an attribute the query fixed, so a system command is a leaf of
// the dataflow, not a stage of it. None is chunked either - each runs exactly
// once at function scope and materializes its whole (small) result table in
// place, the way db.count_result materializes its single row - so the lowering
// opens no loop for one. And none is Pure: the side effect is the point, so a
// command whose result no db.output reads must still survive dead-code
// elimination.
class SystemOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, traits>;

/**
* LoadGraph
*/
def LoadGraph : SystemOp<"load_graph"> {
  let summary = "Load a named graph from the data directory into memory";
  let description = [{
    `LOAD GRAPH social`. Reads the graph's dumped commits off disk and publishes
    it under `graph_name`, then reports that name as its single result row.

      %g = db.load_graph("social") : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$graph_name);
  let results   = (outs ColumnStrings:$graph);

  let assemblyFormat = "`(` $graph_name `)` attr-dict `:` qualified(type($graph))";
}

/**
* CreateGraph
*/
def CreateGraph : SystemOp<"create_graph"> {
  let summary = "Create an empty graph under a given name";
  let description = [{
    `CREATE GRAPH social`. Registers a new, empty graph in the system and reports
    its name as its single result row - the empty-graph sibling of db.load_graph.

      %g = db.create_graph("social") : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$graph_name);
  let results   = (outs ColumnStrings:$graph);

  let assemblyFormat = "`(` $graph_name `)` attr-dict `:` qualified(type($graph))";
}

/**
* ImportGraph
*/
def ImportGraph : SystemOp<"import_graph"> {
  let summary = "Build a graph by importing a file of a declared format";
  let description = [{
    The common op behind `LOAD JSONL`, `LOAD GML` and `LOAD PARQUET`: all three
    read a file at `path` into a new graph named `graph_name` through the same
    importer, which decides the actual format from the file itself. `format` keeps
    which of the three the query wrote, so a failed import is reported against the
    statement the user typed.

      %g = db.import_graph jsonl("/data/social.jsonl") as "social" : !db.column<!storage.string>

    `embeddings` is the optional `WITH EMBEDDINGS [{prop, dim}, ...]` clause of
    `LOAD JSONL`: one entry per property that holds a vector, mapping the
    property name to its dimension. The other two formats never carry it.

      %g = db.import_graph jsonl("/data/social.jsonl") as "social"
             embeddings {vec = 128 : ui64} : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$path,
                       StrAttr:$graph_name,
                       GraphImportFormat:$format,
                       OptionalAttr<DictionaryAttr>:$embeddings);
  let results   = (outs ColumnStrings:$graph);

  let assemblyFormat = [{
    $format `(` $path `)` `as` $graph_name (`embeddings` $embeddings^)?
    attr-dict `:` qualified(type($graph))
  }];
}

/**
* ListGraphs
*/
def ListGraphs : SystemOp<"list_graphs"> {
  let summary = "Name every graph currently loaded in memory";
  let description = [{
    `LIST GRAPH`. One row per loaded graph, holding its name.

      %names = db.list_graphs : !db.column<!storage.string>
  }];

  let results = (outs ColumnStrings:$graphs);

  let assemblyFormat = "attr-dict `:` qualified(type($graphs))";
}

/**
* ListAvailableGraphs
*/
def ListAvailableGraphs : SystemOp<"list_available_graphs"> {
  let summary = "Name every graph on disk, with its current load state";
  let description = [{
    `LIST AVAILABLE GRAPHS`. The on-disk counterpart of db.list_graphs: one row
    per graph in the data directory - loaded or not - carrying its name and the
    two flags that say whether it is already in memory and whether a load of it
    is in flight. The three columns are row-aligned.

      %names, %loaded, %loading = db.list_available_graphs
        : !db.column<!storage.string>, !db.column<!storage.bool>, !db.column<!storage.bool>
  }];

  let results = (outs ColumnStrings:$graphs, ColumnBools:$loaded, ColumnBools:$loading);

  let assemblyFormat = [{
    attr-dict `:` qualified(type($graphs)) `,` qualified(type($loaded)) `,` qualified(type($loading))
  }];
}

/**
* ChangeCommand
*/
def ChangeCommand : SystemOp<"change"> {
  let summary = "Create, submit, delete or list the changes of a graph";
  let description = [{
    `CHANGE NEW` / `SUBMIT` / `DELETE` / `LIST`, selected by `change_operation`. All four
    report change IDs, so all four share one op: new/submit/delete each act on a
    single change and report its ID; list reports one row per open change.

      %ids = db.change new : !db.column<!storage.change_id>

    Submit and delete need the session to have a change selected - they act on
    the transaction's own change - and fail when it has none.
  }];

  let arguments = (ins ChangeOperation:$change_operation);
  let results   = (outs ColumnChangeIDs:$changes);

  let assemblyFormat = "$change_operation attr-dict `:` qualified(type($changes))";
}

/**
* CommitChange
*/
def CommitChange : SystemOp<"commit"> {
  let summary = "Commit the writes buffered in the session's change";
  let description = [{
    `COMMIT`. Turns the change's buffered writes into a commit, which is what
    makes them visible to the reads that follow in the same change. It reports
    nothing, so no db.output follows it.

      db.commit
  }];

  let assemblyFormat = "attr-dict";
}

/**
* LoadCommit
*/
def LoadCommit : SystemOp<"load_commit"> {
  let summary = "Load one commit of the session's graph from disk";
  let description = [{
    `LOAD COMMIT "a1b2c3"`. Brings a single dumped commit of the selected graph
    back into memory so a query can check it out. `commit_hash` is the hash
    spelled in the query - hexadecimal, or `head` - and is parsed at execution,
    where an unparsable one is reported against the query.

      db.load_commit("a1b2c3")
  }];

  let arguments = (ins StrAttr:$commit_hash);

  let assemblyFormat = "`(` $commit_hash `)` attr-dict";
}

/**
* MergeDataParts
*/
def MergeDataParts : SystemOp<"merge_dataparts"> {
  let summary = "Merge the data parts of the session's graph into fewer, larger ones";
  let description = [{
    `MERGE_DATAPARTS`. A maintenance command: it compacts the graph's immutable
    data parts, leaving the rows unchanged. It reports nothing.

      db.merge_dataparts
  }];

  let assemblyFormat = "attr-dict";
}

/**
* S3Connect
*/
def S3Connect : SystemOp<"s3_connect"> {
  let summary = "Open the session's S3 client with a set of credentials";
  let description = [{
    `S3 CONNECT "<id>" "<secret>" "<region>"`. Creates the client the transfers
    then use; it reports nothing.

      db.s3_connect("AKIA...", "secret", "eu-west-1")
  }];

  let arguments = (ins StrAttr:$access_id, StrAttr:$secret_key, StrAttr:$aws_region);

  let assemblyFormat = "`(` $access_id `,` $secret_key `,` $aws_region `)` attr-dict";
}

/**
* S3Transfer
*/
def S3Transfer : SystemOp<"s3_transfer"> {
  let summary = "Move files between the data directory and an S3 bucket";
  let description = [{
    `S3 PULL` / `S3 PUSH`, selected by `direction`. The S3 URL of the query is
    split by the analyzer into `bucket` plus either `file` (a single object) or
    `prefix` (a directory); exactly one of the two is set, and which one decides
    whether the transfer moves one file or a whole tree. `local_path` is relative
    to the data directory, which the transfer refuses to escape. It reports
    nothing.

      db.s3_transfer pull("bucket", "graphs/", "", "graphs")
  }];

  let arguments = (ins S3TransferDirection:$direction,
                       StrAttr:$bucket,
                       StrAttr:$prefix,
                       StrAttr:$file,
                       StrAttr:$local_path);

  let assemblyFormat = [{
    $direction `(` $bucket `,` $prefix `,` $file `,` $local_path `)` attr-dict
  }];
}

/**
* ShowProcedures
*/
def ShowProcedures : SystemOp<"show_procedures"> {
  let summary = "Name every callable procedure, with its signature";
  let description = [{
    `SHOW PROCEDURES`. One row per procedure of every namespace, carrying its
    fully qualified name and its rendered signature. The two columns are
    row-aligned.

      %names, %signatures = db.show_procedures
        : !db.column<!storage.string>, !db.column<!storage.string>
  }];

  let results = (outs ColumnStrings:$names, ColumnStrings:$signatures);

  let assemblyFormat = "attr-dict `:` qualified(type($names)) `,` qualified(type($signatures))";
}

/**
* InstallExtension
*/
def InstallExtension : SystemOp<"install_extension"> {
  let summary = "Install an extension into the running server";
  let description = [{
    `INSTALL <name>`. Loads the extension and reports its name as its single
    result row.

      %e = db.install_extension("geo") : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$extension_name);
  let results   = (outs ColumnStrings:$extension);

  let assemblyFormat = "`(` $extension_name `)` attr-dict `:` qualified(type($extension))";
}

/**
* ShowExtensions
*/
def ShowExtensions : SystemOp<"show_extensions"> {
  let summary = "Name every installed extension";
  let description = [{
    `SHOW EXTENSIONS`. One row per installed extension, holding its name.

      %names = db.show_extensions : !db.column<!storage.string>
  }];

  let results = (outs ColumnStrings:$names);

  let assemblyFormat = "attr-dict `:` qualified(type($names))";
}

/**
* CreateVectorIndex
*/
def CreateVectorIndex : SystemOp<"create_vector_index"> {
  let summary = "Create a vector index of a given dimension and metric";
  let description = [{
    `CREATE VECTOR INDEX vec1 WITH DIMENSION 128 METRIC EUCLID TYPE HNSW`. Creates
    the library the vector searches then query, and reports its name as its
    single result row. `index_kind` defaults to flat when the query omits `TYPE`.

      %i = db.create_vector_index("vec1", 128, euclidean, hnsw) : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$index_name,
                       UI64Attr:$dimension,
                       VectorMetric:$metric,
                       VectorIndexKind:$index_kind);
  let results   = (outs ColumnStrings:$index);

  let assemblyFormat = [{
    `(` $index_name `,` $dimension `,` $metric `,` $index_kind `)`
    attr-dict `:` qualified(type($index))
  }];
}

/**
* DeleteVectorIndex
*/
def DeleteVectorIndex : SystemOp<"delete_vector_index"> {
  let summary = "Delete a vector index";
  let description = [{
    `DELETE VECTOR INDEX vec1`. Drops the library and reports its name as its
    single result row.

      %i = db.delete_vector_index("vec1") : !db.column<!storage.string>
  }];

  let arguments = (ins StrAttr:$index_name);
  let results   = (outs ColumnStrings:$index);

  let assemblyFormat = "`(` $index_name `)` attr-dict `:` qualified(type($index))";
}

/**
* ShowVectorIndexes
*/
def ShowVectorIndexes : SystemOp<"show_vector_indexes"> {
  let summary = "Name every vector index, with its dimension";
  let description = [{
    `SHOW VECTOR INDEXES`. One row per library, carrying its name and the
    dimension of the vectors it holds. The two columns are row-aligned.

      %names, %dimensions = db.show_vector_indexes
        : !db.column<!storage.string>, !db.column<ui64>
  }];

  let results = (outs ColumnStrings:$names, ColumnCounts:$dimensions);

  let assemblyFormat = "attr-dict `:` qualified(type($names)) `,` qualified(type($dimensions))";
}

/**
* LoadVector
*/
def LoadVector : SystemOp<"load_vector"> {
  let summary = "Load vectors from a CSV file into a vector index";
  let description = [{
    `LOAD VECTOR FROM "vectors.csv" IN vec1`. Reads one `id,dim1,...,dimN` row per
    vector from a path relative to the data directory, adds them to the named
    index, and reports how many it added as its single result row.

      %n = db.load_vector("vectors.csv", "vec1") : !db.column<ui64>
  }];

  let arguments = (ins StrAttr:$path, StrAttr:$index_name);
  let results   = (outs ColumnCounts:$count);

  let assemblyFormat = "`(` $path `,` $index_name `)` attr-dict `:` qualified(type($count))";
}

/**
* LoadEmbedding
*/
def LoadEmbedding : SystemOp<"load_embedding"> {
  let summary = "Load node embeddings from a Parquet file into a node property";
  let description = [{
    `LOAD EMBEDDING FROM "emb.parquet" AS vec`. Reads a node ID and an embedding
    per row from a path relative to the data directory and stages one property
    write per node, so - unlike db.load_vector, which writes to a vector index -
    it needs an open change and its writes become visible at the next COMMIT. It
    reports how many nodes it wrote as its single result row.

      %n = db.load_embedding("emb.parquet", "vec") : !db.column<ui64>
  }];

  let arguments = (ins StrAttr:$path, StrAttr:$property_name);
  let results   = (outs ColumnCounts:$count);

  let assemblyFormat = "`(` $path `,` $property_name `)` attr-dict `:` qualified(type($count))";
}

/**
* CreatePropertyIndex
*/
def CreatePropertyIndex : SystemOp<"create_property_index"> {
  let summary = "Declare a property index over the nodes or the edges";
  let description = [{
    `CREATE INDEX byName FOR (n) ON n.name`, and its `FOR [e]` edge form, selected
    by `entity`. The index is built from the current graph and staged on the
    session's change, so it takes effect at the next COMMIT. It reports nothing.

      db.create_property_index("byName", "name", node)
  }];

  let arguments = (ins StrAttr:$index_name, StrAttr:$property_name, IndexedEntity:$entity);

  let assemblyFormat = "`(` $index_name `,` $property_name `,` $entity `)` attr-dict";
}

/**
* DropIndex
*/
def DropIndex : SystemOp<"drop_index"> {
  let summary = "Drop a property index by name";
  let description = [{
    `DROP INDEX byName`. Stages the drop on the session's change, so - like
    db.create_property_index - it takes effect at the next COMMIT. It reports
    nothing.

      db.drop_index("byName")
  }];

  let arguments = (ins StrAttr:$index_name);

  let assemblyFormat = "`(` $index_name `)` attr-dict";
}

/**
* Explain
*/
def Explain : SystemOp<"explain"> {
  let summary = "Report the compilation stages an EXPLAIN prefix asked for";
  let description = [{
    `EXPLAIN <query>`. The stages are rendered while the explained query is
    compiled - its dependency graphs, its db program before and after each
    optimisation pass, the nl program it lowers to - so the op reports text the
    compiler already produced rather than computing anything. Its two columns are
    row-aligned: one row per dump, in the order the compilation produced them.

      %stages, %dumps = db.explain(["db"], ["module {...}"])
        : !db.column<!storage.string>, !db.column<!storage.string>
  }];

  let arguments = (ins StrArrayAttr:$stage_names, StrArrayAttr:$stage_dumps);
  let results   = (outs ColumnStrings:$stages, ColumnStrings:$dumps);

  let assemblyFormat = [{
    `(` $stage_names `,` $stage_dumps `)` attr-dict `:`
    qualified(type($stages)) `,` qualified(type($dumps))
  }];
}

#endif // TURINGDB_SYSTEM_OPS
