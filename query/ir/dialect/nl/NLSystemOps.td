#ifndef TURINGDB_NL_SYSTEM_OPS
#define TURINGDB_NL_SYSTEM_OPS

include "NLDialect.td"
include "NLTypes.td"
include "StorageEnums.td"

// The nl siblings of the db system-command family.
//
// Every other db op is rewritten on the way down - a scan becomes an iterator
// plus a loop, a property fetch is resolved against the schema. A system command
// is not: it has no column operand to chunk, no name to resolve and no loop to
// open, so the lowering copies it across one for one, turning each db column
// result into the chunk the command fills. Each op therefore carries exactly the
// attributes of its db counterpart, under the same names.
//
// Like their db siblings none is Pure: the side effect is the point.
class NLSystemOp<string mnemonic, list<Trait> traits = []>
    : NLOp<mnemonic, traits>;

/**
* LoadGraph
*/
def LoadGraph : NLSystemOp<"load_graph"> {
  let summary = "Load a named graph from the data directory into memory";
  let description = [{
    Imperative counterpart of db.load_graph. Runs once at function scope and
    fills its chunk with the single row naming the graph it loaded.

      %g = nl.load_graph("social") : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$graph_name);
  let results   = (outs NLChunk:$graph);

  let assemblyFormat = "`(` $graph_name `)` attr-dict `:` qualified(type($graph))";
}

/**
* CreateGraph
*/
def CreateGraph : NLSystemOp<"create_graph"> {
  let summary = "Create an empty graph under a given name";
  let description = [{
    Imperative counterpart of db.create_graph. Fills its chunk with the single
    row naming the graph it created.

      %g = nl.create_graph("social") : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$graph_name);
  let results   = (outs NLChunk:$graph);

  let assemblyFormat = "`(` $graph_name `)` attr-dict `:` qualified(type($graph))";
}

/**
* ImportGraph
*/
def ImportGraph : NLSystemOp<"import_graph"> {
  let summary = "Build a graph by importing a file of a declared format";
  let description = [{
    Imperative counterpart of db.import_graph. Fills its chunk with the single
    row naming the graph it built.

      %g = nl.import_graph jsonl("/data/social.jsonl") as "social" : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$path,
                       StrAttr:$graph_name,
                       GraphImportFormat:$format,
                       OptionalAttr<DictionaryAttr>:$embeddings);
  let results   = (outs NLChunk:$graph);

  let assemblyFormat = [{
    $format `(` $path `)` `as` $graph_name (`embeddings` $embeddings^)?
    attr-dict `:` qualified(type($graph))
  }];
}

/**
* ListGraphs
*/
def ListGraphs : NLSystemOp<"list_graphs"> {
  let summary = "Name every graph currently loaded in memory";
  let description = [{
    Imperative counterpart of db.list_graphs. Fills its chunk with one row per
    loaded graph.

      %names = nl.list_graphs : !nl.chunk<!storage.string>
  }];

  let results = (outs NLChunk:$graphs);

  let assemblyFormat = "attr-dict `:` qualified(type($graphs))";
}

/**
* ListAvailableGraphs
*/
def ListAvailableGraphs : NLSystemOp<"list_available_graphs"> {
  let summary = "Name every graph on disk, with its current load state";
  let description = [{
    Imperative counterpart of db.list_available_graphs. Fills its three
    row-aligned chunks with one row per on-disk graph.

      %names, %loaded, %loading = nl.list_available_graphs
        : !nl.chunk<!storage.string>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>
  }];

  let results = (outs NLChunk:$graphs, NLChunk:$loaded, NLChunk:$loading);

  let assemblyFormat = [{
    attr-dict `:` qualified(type($graphs)) `,` qualified(type($loaded)) `,` qualified(type($loading))
  }];
}

/**
* ChangeCommand
*/
def ChangeCommand : NLSystemOp<"change"> {
  let summary = "Create, submit, delete or list the changes of a graph";
  let description = [{
    Imperative counterpart of db.change. Fills its chunk with the ID of the
    change the operation acted on, or - for list - one row per open change.

      %ids = nl.change new : !nl.chunk<!storage.change_id>
  }];

  let arguments = (ins ChangeOperation:$change_operation);
  let results   = (outs NLChunk:$changes);

  let assemblyFormat = "$change_operation attr-dict `:` qualified(type($changes))";
}

/**
* CommitChange
*/
def CommitChange : NLSystemOp<"commit"> {
  let summary = "Commit the writes buffered in the session's change";
  let description = [{
    Imperative counterpart of db.commit. Produces no chunk.

      nl.commit
  }];

  let assemblyFormat = "attr-dict";
}

/**
* LoadCommit
*/
def LoadCommit : NLSystemOp<"load_commit"> {
  let summary = "Load one commit of the session's graph from disk";
  let description = [{
    Imperative counterpart of db.load_commit. Parses the hash and loads the
    commit; produces no chunk.

      nl.load_commit("a1b2c3")
  }];

  let arguments = (ins StrAttr:$commit_hash);

  let assemblyFormat = "`(` $commit_hash `)` attr-dict";
}

/**
* MergeDataParts
*/
def MergeDataParts : NLSystemOp<"merge_dataparts"> {
  let summary = "Merge the data parts of the session's graph into fewer, larger ones";
  let description = [{
    Imperative counterpart of db.merge_dataparts. Produces no chunk.

      nl.merge_dataparts
  }];

  let assemblyFormat = "attr-dict";
}

/**
* S3Connect
*/
def S3Connect : NLSystemOp<"s3_connect"> {
  let summary = "Open the session's S3 client with a set of credentials";
  let description = [{
    Imperative counterpart of db.s3_connect. Produces no chunk.

      nl.s3_connect("AKIA...", "secret", "eu-west-1")
  }];

  let arguments = (ins StrAttr:$access_id, StrAttr:$secret_key, StrAttr:$aws_region);

  let assemblyFormat = "`(` $access_id `,` $secret_key `,` $aws_region `)` attr-dict";
}

/**
* S3Transfer
*/
def S3Transfer : NLSystemOp<"s3_transfer"> {
  let summary = "Move files between the data directory and an S3 bucket";
  let description = [{
    Imperative counterpart of db.s3_transfer. Produces no chunk.

      nl.s3_transfer pull("bucket", "graphs/", "", "graphs")
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
def ShowProcedures : NLSystemOp<"show_procedures"> {
  let summary = "Name every callable procedure, with its signature";
  let description = [{
    Imperative counterpart of db.show_procedures. Fills its two row-aligned
    chunks with one row per procedure.

      %names, %signatures = nl.show_procedures
        : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
  }];

  let results = (outs NLChunk:$names, NLChunk:$signatures);

  let assemblyFormat = "attr-dict `:` qualified(type($names)) `,` qualified(type($signatures))";
}

/**
* InstallExtension
*/
def InstallExtension : NLSystemOp<"install_extension"> {
  let summary = "Install an extension into the running server";
  let description = [{
    Imperative counterpart of db.install_extension. Fills its chunk with the
    single row naming the extension it installed.

      %e = nl.install_extension("geo") : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$extension_name);
  let results   = (outs NLChunk:$extension);

  let assemblyFormat = "`(` $extension_name `)` attr-dict `:` qualified(type($extension))";
}

/**
* ShowExtensions
*/
def ShowExtensions : NLSystemOp<"show_extensions"> {
  let summary = "Name every installed extension";
  let description = [{
    Imperative counterpart of db.show_extensions. Fills its chunk with one row
    per installed extension.

      %names = nl.show_extensions : !nl.chunk<!storage.string>
  }];

  let results = (outs NLChunk:$names);

  let assemblyFormat = "attr-dict `:` qualified(type($names))";
}

/**
* CreateVectorIndex
*/
def CreateVectorIndex : NLSystemOp<"create_vector_index"> {
  let summary = "Create a vector index of a given dimension and metric";
  let description = [{
    Imperative counterpart of db.create_vector_index. Fills its chunk with the
    single row naming the index it created.

      %i = nl.create_vector_index("vec1", 128, euclidean, hnsw) : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$index_name,
                       UI64Attr:$dimension,
                       VectorMetric:$metric,
                       VectorIndexKind:$index_kind);
  let results   = (outs NLChunk:$index);

  let assemblyFormat = [{
    `(` $index_name `,` $dimension `,` $metric `,` $index_kind `)`
    attr-dict `:` qualified(type($index))
  }];
}

/**
* DeleteVectorIndex
*/
def DeleteVectorIndex : NLSystemOp<"delete_vector_index"> {
  let summary = "Delete a vector index";
  let description = [{
    Imperative counterpart of db.delete_vector_index. Fills its chunk with the
    single row naming the index it dropped.

      %i = nl.delete_vector_index("vec1") : !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrAttr:$index_name);
  let results   = (outs NLChunk:$index);

  let assemblyFormat = "`(` $index_name `)` attr-dict `:` qualified(type($index))";
}

/**
* ShowVectorIndexes
*/
def ShowVectorIndexes : NLSystemOp<"show_vector_indexes"> {
  let summary = "Name every vector index, with its dimension";
  let description = [{
    Imperative counterpart of db.show_vector_indexes. Fills its two row-aligned
    chunks with one row per library.

      %names, %dimensions = nl.show_vector_indexes
        : !nl.chunk<!storage.string>, !nl.chunk<ui64>
  }];

  let results = (outs NLChunk:$names, NLChunk:$dimensions);

  let assemblyFormat = "attr-dict `:` qualified(type($names)) `,` qualified(type($dimensions))";
}

/**
* LoadVector
*/
def LoadVector : NLSystemOp<"load_vector"> {
  let summary = "Load vectors from a CSV file into a vector index";
  let description = [{
    Imperative counterpart of db.load_vector. Fills its chunk with the single row
    counting the vectors it added.

      %n = nl.load_vector("vectors.csv", "vec1") : !nl.chunk<ui64>
  }];

  let arguments = (ins StrAttr:$path, StrAttr:$index_name);
  let results   = (outs NLChunk:$count);

  let assemblyFormat = "`(` $path `,` $index_name `)` attr-dict `:` qualified(type($count))";
}

/**
* LoadEmbedding
*/
def LoadEmbedding : NLSystemOp<"load_embedding"> {
  let summary = "Load node embeddings from a Parquet file into a node property";
  let description = [{
    Imperative counterpart of db.load_embedding. Stages one property write per
    node into the change's write buffer and fills its chunk with the single row
    counting them.

      %n = nl.load_embedding("emb.parquet", "vec") : !nl.chunk<ui64>
  }];

  let arguments = (ins StrAttr:$path, StrAttr:$property_name);
  let results   = (outs NLChunk:$count);

  let assemblyFormat = "`(` $path `,` $property_name `)` attr-dict `:` qualified(type($count))";
}

/**
* CreatePropertyIndex
*/
def CreatePropertyIndex : NLSystemOp<"create_property_index"> {
  let summary = "Declare a property index over the nodes or the edges";
  let description = [{
    Imperative counterpart of db.create_property_index. Builds the index and
    stages it on the change; produces no chunk.

      nl.create_property_index("byName", "name", node)
  }];

  let arguments = (ins StrAttr:$index_name, StrAttr:$property_name, IndexedEntity:$entity);

  let assemblyFormat = "`(` $index_name `,` $property_name `,` $entity `)` attr-dict";
}

/**
* DropIndex
*/
def DropIndex : NLSystemOp<"drop_index"> {
  let summary = "Drop a property index by name";
  let description = [{
    Imperative counterpart of db.drop_index. Stages the drop on the change;
    produces no chunk.

      nl.drop_index("byName")
  }];

  let arguments = (ins StrAttr:$index_name);

  let assemblyFormat = "`(` $index_name `)` attr-dict";
}

/**
* Explain
*/
def Explain : NLSystemOp<"explain"> {
  let summary = "Report the compilation stages an EXPLAIN prefix asked for";
  let description = [{
    Imperative counterpart of db.explain. Fills its two row-aligned chunks with
    one row per dump the compilation of the explained query produced.

      %stages, %dumps = nl.explain(["db"], ["module {...}"])
        : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
  }];

  let arguments = (ins StrArrayAttr:$stage_names, StrArrayAttr:$stage_dumps);
  let results   = (outs NLChunk:$stages, NLChunk:$dumps);

  let assemblyFormat = [{
    `(` $stage_names `,` $stage_dumps `)` attr-dict `:`
    qualified(type($stages)) `,` qualified(type($dumps))
  }];
}

#endif // TURINGDB_NL_SYSTEM_OPS
