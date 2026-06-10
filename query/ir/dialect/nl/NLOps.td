#ifndef TURINGDB_NL_OPS
#define TURINGDB_NL_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* ScanNodes
*/
def ScanNodes : NLOp<"scan_nodes", [Pure]> {
  let summary = "Create a database iterator over all nodes in the graph";
  let description = [{
    Imperative counterpart of db.scan_nodes. Each step of an enclosing
    nl.for fills one chunk of node IDs.
  }];
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(`operands`)` attr-dict `:` qualified(type($result))";
  let hasVerifier = 1;
}

/**
* GetOutEdges
*/
def GetOutEdges : NLOp<"get_out_edges", [Pure]> {
  let summary = "Create a database iterator over the out-edges of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs.
  }];
  let arguments = (ins NLChunk:$input_nodes);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $input_nodes `)` attr-dict `:` functional-type($input_nodes, $result)";
  let hasVerifier = 1;
}

/**
* For
*/
def For : NLOp<"for", [SingleBlockImplicitTerminator<"Yield">]> {
  let summary = "Drive a database iterator chunk by chunk";
  let description = [{
    Runs the body once per iterator step until the iterator is exhausted.
    Each step fills the next chunks of the iterator and binds them to the
    loop variables, one variable per chunk:

    nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
        ...
    }
  }];
  let arguments = (ins NLIterator:$iterator);
  let regions = (region SizedRegion<1>:$bodyRegion);
  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;
}

/**
* Yield
*/
def Yield : NLOp<"yield", [Pure, Terminator, HasParent<"For">]> {
  let summary = "End one step of an nl.for loop";
  let assemblyFormat = "attr-dict";
}

#endif //TURINGDB_NL_OPS
