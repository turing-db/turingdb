#ifndef TURINGDB_NL_OPS
#define TURINGDB_NL_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* ScanNodes
*/
def ScanNodes : NLOp<"scan_nodes", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all nodes in the graph";
  let description = [{
    Imperative counterpart of db.scan_nodes. Each step of an enclosing
    nl.for fills one chunk of node IDs. The result type is always
    !nl.iter<!nl.chunk<!nl.node_id>> and is inferred, not spelled out.
  }];
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(`operands`)` attr-dict";
}

/**
* GetOutEdges
*/
def GetOutEdges : NLOp<"get_out_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs. Both the input chunk type and the result iterator type
    are fixed and inferred, not spelled out.
  }];
  let arguments = (ins NLNodeIDChunk:$input_nodes);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $input_nodes `)` attr-dict";
}

/**
* GetInEdges
*/
def GetInEdges : NLOp<"get_in_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of a chunk of nodes";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges: where that walks each
    node's successors, this walks its predecessors. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs - the same chunk shape as out-edges, so the two are
    interchangeable in a loop. Both the input chunk type and the result
    iterator type are fixed and inferred, not spelled out.
  }];
  let arguments = (ins NLNodeIDChunk:$input_nodes);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $input_nodes `)` attr-dict";
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

  // The only builder takes just the iterator value: the loop variables are
  // created by looking up the chunk types on the iterator type, so C++ users
  // never spell any type
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::Value":$iterator)>
  ];
}

/**
* Yield
*/
def Yield : NLOp<"yield", [Pure, Terminator, HasParent<"For">]> {
  let summary = "End one step of an nl.for loop";
  let assemblyFormat = "attr-dict";
}

/**
* Output
*/
def Output : NLOp<"output"> {
  let summary = "Send a row of chunks out as columns of the query result";
  let description = [{
    The sink of the nested loop, mirroring a result builder (LingoDB's
    dsa.append): inside an nl.for body it hands the current chunks to the
    query result, one output column per chunk. The columns are typically loop
    variables, so each nl.for step emits one chunk per column:

    nl.output(%targets) : !nl.chunk<!nl.node_id>
    nl.output(%sources, %targets) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>

    Variadic<NLChunk> accepts any number of chunks of any element type. The op
    is deliberately not Pure: emitting output is its whole point, so it must
    survive dead-code elimination even though it has no results.
  }];
  let arguments = (ins Variadic<NLChunk>:$columns);

  // The chunk element types vary from call to call and - unlike the source
  // ops - cannot be inferred, so they are spelled after the colon. This keeps
  // the text self-describing: the parser builds each operand's type from the
  // printed list rather than looking it up on the defining nl.for (which MLIR
  // cannot do during a parse), the same reason nl.for keeps its : !nl.iter<...>
  // annotation. qualified(...) forces the leading !nl. on every printed type.
  let assemblyFormat = "`(` $columns `)` attr-dict `:` qualified(type($columns))";
}

#endif //TURINGDB_NL_OPS
