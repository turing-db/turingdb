// Generated nl-dialect lowering of cascaded_merge.mlir (db dialect).
// Reproduce with: mlir -f cascaded_merge.mlir -l -g <graph>
// This is the DBLowering output; edit cascaded_merge.mlir, not this file.
//
// MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b) RETURN count(*). One hop per
// pattern, so three nested loops - each one leaving from the same `a` (the outer loop's
// srcids, not the previous hop's target) and carrying every column of the hops above it.
// The three copies of `b` therefore all reach the innermost body, where the merges can
// compare them.
//
// A merge is a per-chunk filter, nothing more: nl.eq over two copies of `b` and an
// nl.filter of every live chunk. The cascade is the chain of two - the first pairs e2's
// copy with KNOWS_WELL's, the second pairs e1's against the first's survivor - and needs
// no state across chunks, so it lowers entirely inside the innermost loop.
module {
  func.func @main() {
    %0 = nl.count
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 {
      // Hop 1, (a)-[e2]->(b): %arg1 is `a`, %arg4 is e2's copy of `b`.
      %3 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %3 {
        // Hop 2, (a)-[e1]->(b): input is %arg1, the same `a`. %arg8 is e1's copy of `b`,
        // %arg10 is e2's carried through.
        %4 = nl.get_out_edges(%arg1, {%arg2, %arg4, %arg3})
        nl.for %arg5, %arg6, %arg7, %arg8, %arg9, %arg10, %arg11 in %4 {
          // Hop 3, (a)-[:KNOWS_WELL]->(b): %arg15 is its copy of `b`, joining e2's
          // (%arg17) and e1's (%arg19) in this body.
          %5 = nl.get_out_edges(%arg5, {%arg9, %arg10, %arg6, %arg8, %arg7, %arg11})
          nl.for %arg12, %arg13, %arg14, %arg15, %arg16, %arg17, %arg18, %arg19, %arg20, %arg21 in %5 {
            // The KNOWS_WELL constraint, on hop 3's edge type.
            %6 = nl.check_edge_type_constraint(%arg14, ["KNOWS_WELL"])
            %7:10 = nl.filter %6, (%arg13, %arg15, %arg18, %arg19, %arg16, %arg17, %arg12, %arg14, %arg20, %arg21)

            // First merge: e2's copy of `b` (%7#5) against KNOWS_WELL's (%7#1). Its
            // survivor is carried twice, once as itself and once as the intermediate
            // merge node the cascade compares next.
            %8 = nl.eq %7#5, %7#1
            %9:11 = nl.filter %8, (%7#0, %7#1, %7#2, %7#5, %7#3, %7#4, %7#5, %7#6, %7#7, %7#8, %7#9)

            // Second merge: e1's copy (%9#4) against that intermediate node (%9#3). Past
            // it the three copies are one node - `b`.
            %10 = nl.eq %9#4, %9#3
            %11:12 = nl.filter %10, (%9#4, %9#0, %9#1, %9#2, %9#3, %9#4, %9#5, %9#6, %9#7, %9#8, %9#9, %9#10)

            // count(*) tallies the rows of `a` (%11#8) that survived both merges.
            nl.count_update %0, %11#8
          }
        }
      }
    }
    %2 = nl.count_result(%0)
    nl.output(%2) names ["count(*)"]
    return
  }
}
