// The system-level statements in the db dialect: the commands that act on the
// server rather than on the graph's rows.
//
// Unlike every other db op they take no column operand - each parameter is an
// attribute the query fixed - and each is the whole program, so one function per
// command here rather than one nest with many stages. A command that reports rows
// is followed by the db.output naming them; the rest report nothing and have none.
module {
  // LOAD GRAPH social
  func.func @main() {
    %g = db.load_graph("social") : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // CREATE GRAPH social
  func.func @create_graph() {
    %g = db.create_graph("social") : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // LOAD GML "/data/social.gml" AS social
  func.func @import_gml() {
    %g = db.import_graph gml("/data/social.gml") as "social" : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // LOAD PARQUET "/data/social" AS social
  func.func @import_parquet() {
    %g = db.import_graph parquet("/data/social") as "social" : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // LOAD JSONL "/data/social.jsonl" AS social WITH EMBEDDINGS [{vector, 128}]
  func.func @import_jsonl_with_embeddings() {
    %g = db.import_graph jsonl("/data/social.jsonl") as "social" embeddings {vector = 128 : ui64}
       : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // LIST GRAPH
  func.func @list_graphs() {
    %g = db.list_graphs : !db.column<!storage.string>
    db.output(%g) names ["graphName"] : !db.column<!storage.string>
    return
  }

  // LIST AVAILABLE GRAPHS
  func.func @list_available_graphs() {
    %g, %loaded, %loading = db.list_available_graphs
       : !db.column<!storage.string>, !db.column<!storage.bool>, !db.column<!storage.bool>
    db.output(%g, %loaded, %loading) names ["graphName", "isLoaded", "isLoading"]
       : !db.column<!storage.string>, !db.column<!storage.bool>, !db.column<!storage.bool>
    return
  }

  // CHANGE NEW - and, under the other three keywords, CHANGE SUBMIT / DELETE / LIST
  func.func @change_new() {
    %ids = db.change new : !db.column<!storage.change_id>
    db.output(%ids) names ["changeID"] : !db.column<!storage.change_id>
    return
  }

  // CHANGE LIST
  func.func @change_list() {
    %ids = db.change list : !db.column<!storage.change_id>
    db.output(%ids) names ["changeID"] : !db.column<!storage.change_id>
    return
  }

  // CHANGE SUBMIT
  func.func @change_submit() {
    %ids = db.change submit : !db.column<!storage.change_id>
    db.output(%ids) names ["changeID"] : !db.column<!storage.change_id>
    return
  }

  // CHANGE DELETE
  func.func @change_delete() {
    %ids = db.change delete : !db.column<!storage.change_id>
    db.output(%ids) names ["changeID"] : !db.column<!storage.change_id>
    return
  }

  // COMMIT
  func.func @commit() {
    db.commit
    return
  }

  // LOAD COMMIT "a1b2c3"
  func.func @load_commit() {
    db.load_commit("a1b2c3")
    return
  }

  // MERGE_DATAPARTS
  func.func @merge_dataparts() {
    db.merge_dataparts
    return
  }

  // S3 CONNECT "AKIA..." "secret" "eu-west-1"
  func.func @s3_connect() {
    db.s3_connect("AKIA...", "secret", "eu-west-1")
    return
  }

  // S3 PULL "s3://bucket/graphs/" "graphs" - a prefix moves the whole tree, so
  // the file field is empty; a named object sets it and leaves the prefix empty
  func.func @s3_pull() {
    db.s3_transfer pull("bucket", "graphs/", "", "graphs")
    return
  }

  // S3 PUSH "graphs" "s3://bucket/graphs/social.jsonl"
  func.func @s3_push() {
    db.s3_transfer push("bucket", "", "graphs/social.jsonl", "graphs")
    return
  }

  // SHOW PROCEDURES
  func.func @show_procedures() {
    %names, %signatures = db.show_procedures
       : !db.column<!storage.string>, !db.column<!storage.string>
    db.output(%names, %signatures) names ["name", "signature"]
       : !db.column<!storage.string>, !db.column<!storage.string>
    return
  }

  // INSTALL geo
  func.func @install_extension() {
    %e = db.install_extension("geo") : !db.column<!storage.string>
    db.output(%e) names ["extensionName"] : !db.column<!storage.string>
    return
  }

  // SHOW EXTENSIONS
  func.func @show_extensions() {
    %names = db.show_extensions : !db.column<!storage.string>
    db.output(%names) names ["name"] : !db.column<!storage.string>
    return
  }

  // CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC EUCLID TYPE HNSW
  func.func @create_vector_index() {
    %i = db.create_vector_index("vectors", 4, euclidean, hnsw) : !db.column<!storage.string>
    db.output(%i) names ["indexName"] : !db.column<!storage.string>
    return
  }

  // CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC COSINE TYPE FLAT
  func.func @create_vector_index_flat_cosine() {
    %i = db.create_vector_index("vectors", 4, cosine, flat) : !db.column<!storage.string>
    db.output(%i) names ["indexName"] : !db.column<!storage.string>
    return
  }

  // DELETE VECTOR INDEX vectors
  func.func @delete_vector_index() {
    %i = db.delete_vector_index("vectors") : !db.column<!storage.string>
    db.output(%i) names ["indexName"] : !db.column<!storage.string>
    return
  }

  // SHOW VECTOR INDEXES
  func.func @show_vector_indexes() {
    %names, %dimensions = db.show_vector_indexes : !db.column<!storage.string>, !db.column<ui64>
    db.output(%names, %dimensions) names ["name", "dimension"]
       : !db.column<!storage.string>, !db.column<ui64>
    return
  }

  // LOAD VECTOR FROM "vectors.csv" IN vectors
  func.func @load_vector() {
    %n = db.load_vector("vectors.csv", "vectors") : !db.column<ui64>
    db.output(%n) names ["count"] : !db.column<ui64>
    return
  }

  // LOAD EMBEDDING FROM "embeddings.parquet" AS vector
  func.func @load_embedding() {
    %n = db.load_embedding("embeddings.parquet", "vector") : !db.column<ui64>
    db.output(%n) names ["count"] : !db.column<ui64>
    return
  }

  // CREATE INDEX byName FOR (n) ON n.name, and its FOR [e] edge form
  func.func @create_property_index() {
    db.create_property_index("byName", "name", node)
    db.create_property_index("bySince", "since", edge)
    return
  }

  // DROP INDEX byName
  func.func @drop_index() {
    db.drop_index("byName")
    return
  }

  // EXPLAIN MATCH (n) RETURN n: the dumps the explained query's compilation produced,
  // one row apiece, reported by the small program built once that compilation is over
  func.func @explain() {
    %stages, %dumps = db.explain(["codegen", "db"], ["module {...}", "module {...}"])
       : !db.column<!storage.string>, !db.column<!storage.string>
    db.output(%stages, %dumps) names ["stage", "dump"]
       : !db.column<!storage.string>, !db.column<!storage.string>
    return
  }
}
