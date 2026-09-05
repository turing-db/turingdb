// nl-dialect lowering of system_commands.mlir (db dialect).
// Reproduce with: mlir -dump-lowered system_commands.mlir
// This is the DBLowering output; edit system_commands.mlir, not this file.
//
// The system commands are the one family the lowering copies across one for one:
// there is no column to chunk, no name to resolve against the schema and no loop
// to open, so each db op becomes its nl sibling with the same attributes, and each
// db column result becomes the chunk the command fills.
module {
  func.func @main() {
    %0 = nl.load_graph("social") : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @create_graph() {
    %0 = nl.create_graph("social") : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @import_gml() {
    %0 = nl.import_graph gml("/data/social.gml") as "social" : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @import_parquet() {
    %0 = nl.import_graph parquet("/data/social") as "social" : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @import_jsonl_with_embeddings() {
    %0 = nl.import_graph jsonl("/data/social.jsonl") as "social" embeddings {vector = 128 : ui64}
       : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @list_graphs() {
    %0 = nl.list_graphs : !nl.chunk<!storage.string>
    nl.output(%0) names ["graphName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @list_available_graphs() {
    %0, %1, %2 = nl.list_available_graphs
       : !nl.chunk<!storage.string>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>
    nl.output(%0, %1, %2) names ["graphName", "isLoaded", "isLoading"]
       : !nl.chunk<!storage.string>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>
    return
  }

  func.func @change_new() {
    %0 = nl.change new : !nl.chunk<!storage.change_id>
    nl.output(%0) names ["changeID"] : !nl.chunk<!storage.change_id>
    return
  }

  func.func @change_list() {
    %0 = nl.change list : !nl.chunk<!storage.change_id>
    nl.output(%0) names ["changeID"] : !nl.chunk<!storage.change_id>
    return
  }

  func.func @change_submit() {
    %0 = nl.change submit : !nl.chunk<!storage.change_id>
    nl.output(%0) names ["changeID"] : !nl.chunk<!storage.change_id>
    return
  }

  func.func @change_delete() {
    %0 = nl.change delete : !nl.chunk<!storage.change_id>
    nl.output(%0) names ["changeID"] : !nl.chunk<!storage.change_id>
    return
  }

  func.func @commit() {
    nl.commit
    return
  }

  func.func @load_commit() {
    nl.load_commit("a1b2c3")
    return
  }

  func.func @merge_dataparts() {
    nl.merge_dataparts
    return
  }

  func.func @s3_connect() {
    nl.s3_connect("AKIA...", "secret", "eu-west-1")
    return
  }

  func.func @s3_pull() {
    nl.s3_transfer pull("bucket", "graphs/", "", "graphs")
    return
  }

  func.func @s3_push() {
    nl.s3_transfer push("bucket", "", "graphs/social.jsonl", "graphs")
    return
  }

  func.func @show_procedures() {
    %0, %1 = nl.show_procedures : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
    nl.output(%0, %1) names ["name", "signature"]
       : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
    return
  }

  func.func @install_extension() {
    %0 = nl.install_extension("geo") : !nl.chunk<!storage.string>
    nl.output(%0) names ["extensionName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @show_extensions() {
    %0 = nl.show_extensions : !nl.chunk<!storage.string>
    nl.output(%0) names ["name"] : !nl.chunk<!storage.string>
    return
  }

  func.func @create_vector_index() {
    %0 = nl.create_vector_index("vectors", 4, euclidean, hnsw) : !nl.chunk<!storage.string>
    nl.output(%0) names ["indexName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @create_vector_index_flat_cosine() {
    %0 = nl.create_vector_index("vectors", 4, cosine, flat) : !nl.chunk<!storage.string>
    nl.output(%0) names ["indexName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @delete_vector_index() {
    %0 = nl.delete_vector_index("vectors") : !nl.chunk<!storage.string>
    nl.output(%0) names ["indexName"] : !nl.chunk<!storage.string>
    return
  }

  func.func @show_vector_indexes() {
    %0, %1 = nl.show_vector_indexes : !nl.chunk<!storage.string>, !nl.chunk<ui64>
    nl.output(%0, %1) names ["name", "dimension"] : !nl.chunk<!storage.string>, !nl.chunk<ui64>
    return
  }

  func.func @load_vector() {
    %0 = nl.load_vector("vectors.csv", "vectors") : !nl.chunk<ui64>
    nl.output(%0) names ["count"] : !nl.chunk<ui64>
    return
  }

  func.func @load_embedding() {
    %0 = nl.load_embedding("embeddings.parquet", "vector") : !nl.chunk<ui64>
    nl.output(%0) names ["count"] : !nl.chunk<ui64>
    return
  }

  func.func @create_property_index() {
    nl.create_property_index("byName", "name", node)
    nl.create_property_index("bySince", "since", edge)
    return
  }

  func.func @drop_index() {
    nl.drop_index("byName")
    return
  }

  func.func @explain() {
    %stages, %dumps = nl.explain(["codegen", "db"], ["module {...}", "module {...}"])
       : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
    nl.output(%stages, %dumps) names ["stage", "dump"]
       : !nl.chunk<!storage.string>, !nl.chunk<!storage.string>
    return
  }
}
