module {
  func.func @main() {
    // MATCH (a:Person), (b:Interest) RETURN count(*): the row count of a cartesian
    // product of two whole node scans, read from the graph rather than walked.
    //
    // The count_from_metadata pass leaves this behind when the rows a db.count tallies
    // come off nothing but node scans. The scans and the db.cross_product that paired
    // them are gone: how many Person nodes there are, and how many Interest ones, is a
    // number the graph already holds per label, and the product of the two is how many
    // pairs the walk would have produced - 8 * 10 on the SimpleGraph fixture, without
    // building one of the 80 rows.
    //
    // `labels` is one label conjunction per scan, in factor order. An empty conjunction
    // is an unlabelled scan and counts every node, so MATCH (a) RETURN count(*) is
    // db.count_scan_rows([[]]). A label absent from the schema matches no node, so its
    // factor counts 0 and the whole product is 0.
    //
    // It lowers to a single nl.count_scan_rows hoisted to the top of the entry block -
    // it reads no column, so it is loop-invariant the way a constant is - which
    // materializes the single tally row as an unsigned i64 (!nl.chunk<ui64>) for a
    // function-scope nl.output to emit. No loop, no accumulator, no emit step.
    //
    // The property form narrows one scan to the nodes holding a property, which the graph
    // indexes by the label set of its holders - MATCH (a:Person) RETURN count(a.name), the
    // rows where a.name is not null. `of scan` is which of the listed scans it is read
    // from, counted from 0 and defaulting to the first, so MATCH (a:Person), (b:Interest)
    // RETURN count(b.name) names the second:
    //   %count = db.count_scan_rows([["Person"]]) property "name" : !db.column<ui64>
    //   %count = db.count_scan_rows([["Person"], ["Interest"]]) property "name" of scan 1
    //              : !db.column<ui64>
    %count = db.count_scan_rows([["Person"], ["Interest"]]) : !db.column<ui64>

    db.output(%count) : !db.column<ui64>

    return
  }
}
