module {
  func.func @main() {
    // VECTOR SEARCH IN vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids, score RETURN ids, score:
    // the vector-index source. The index, the neighbour count and the query vector are all
    // fixed by the query and it reads no row, so it opens a dataflow of its own, the way
    // db.unwind_const opens one from a literal list.
    //
    // The two results are row-aligned, one row per neighbour, ordered nearest first: the
    // IDs the index holds the vectors under - the IDs a LOAD VECTOR row carried, not node
    // IDs - and the distances the index' metric scored them at. It reports at most as many
    // neighbours as asked for, and fewer when the index holds fewer.
    //
    // Needs no -graph: the index lives in the session's vector database, not in the graph.
    // It does need one to execute, which is why there is no -exec form of this sample.
    //
    // With incoming rows (VECTOR SEARCH ... YIELD ids MATCH (n)) the source is crossed with
    // them through db.cross_product; there is no variant of this op that takes a column.
    %ids, %scores = db.vector_search("vectors", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<i64>, !db.column<f64>

    db.output(%ids, %scores) names ["ids", "score"] : !db.column<i64>, !db.column<f64>

    return
  }
}
