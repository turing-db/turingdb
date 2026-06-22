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
    target node IDs.

    `columns_to_filter` is the carry set, mirroring db.get_out_edges: chunks
    bound by an outer nl.for that this hop must filter down to the rows that
    survive the traversal. Each carried chunk comes back - same type, filtered -
    as a trailing chunk of the result iterator, so an enclosing nl.for binds it
    as an extra loop variable after the four fixed edge chunks:

      %edges = nl.get_out_edges(%a, {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        // second hop b->c, carrying the `a` of this step (here %srcs)
        %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!nl.node_id>
        nl.for %srcs2, %eids2, %etypes2, %c, %aFiltered in %hop { ... }
      }

    The input chunk type and the four fixed result chunks are inferred; only
    the carried chunk types are spelled out, after the colon.
  }];

  // `columns_to_filter`: the variadic carry set, after the fixed input_nodes.
  // NLChunk, not NLNodeIDChunk, so chunks of any element type can be carried.
  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The carried chunk element types vary from call to call and - unlike
  // input_nodes and the inferred result - cannot be built from a recipe, so
  // they are spelled after the colon, the same as nl.output. The optional
  // group (anchored on the carried types) drops the colon when the carry set
  // is empty, keeping the first-hop syntax `(%nodes, {})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
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
    interchangeable in a loop.

    Like nl.get_out_edges it takes a `columns_to_filter` carry set: chunks from
    an outer nl.for that this hop filters and hands back as trailing chunks of
    the result iterator. The input chunk type and the four fixed result chunks
    are inferred; only the carried chunk types are spelled out, after the colon.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetPropertyType
*/
def GetPropertyType : NLOp<"get_property_type", [Pure]> {
  let summary = "Resolve a property name to a property type handle, once, ahead of the loops";
  let description = [{
    Looks the property name up in the graph schema and yields a
    !nl.property_type handle for its storage PropertyTypeID. It takes no chunk,
    so it sits at function scope above the loop nest: a query resolves each name
    once here and feeds the handle to every get_node_properties /
    get_edge_properties step, instead of resolving the name per chunk.

      %weight = nl.get_property_type("weight")

    The result is always !nl.property_type. That type is parameterless and so
    buildable, so the assembly omits it and the parser builds it from the
    recipe - no InferTypeOpAdaptor needed. Pure, so it can be hoisted and shared
    across the loops below it.
  }];

  // The property name, the same `name`/`weight` spelled in the query.
  let arguments = (ins StrAttr:$name);
  let results   = (outs NLPropertyType:$result);

  // No operands and a constant result type, so nothing is printed after the name.
  let assemblyFormat = "`(` $name `)` attr-dict";
}

/**
* GetNodeProperties
*/
def GetNodeProperties : NLOp<"get_node_properties", [Pure]> {
  let summary = "Read one property of a chunk of nodes, as a nullable value chunk";
  let description = [{
    Imperative counterpart of db.get_node_properties. The property is named once
    by nl.get_property_type and passed in as a %property_type handle. It opens no
    loop: it maps the input node chunk to one value per node, in order, straight
    into the current nl.for body.

    A node may not have the property, so the result is a nullable value chunk -
    !nl.chunk<!nl.nullable<...>>, storage's ColumnOptVector. Missing values come
    back null; no row is dropped. The wrapped type is the concrete primitive the
    property resolved to during lowering.

      %weight = nl.get_property_type("weight")
      nl.for %nodes in %scan {
        %values = nl.get_node_properties(%nodes, %weight) : !nl.chunk<!nl.nullable<f64>>
        nl.output(%nodes, %values) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.nullable<f64>>
      }
  }];

  // The node chunk and the property handle are both buildable, so their types
  // are omitted; only the nullable value chunk type is spelled, after the colon.
  let arguments = (ins NLNodeIDChunk:$input_nodes, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_nodes `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : NLOp<"get_edge_properties", [Pure]> {
  let summary = "Read one property of a chunk of edges, as a nullable value chunk";
  let description = [{
    The edge counterpart of get_node_properties: same shape, but the input is a
    chunk of edge IDs and the property is read from the edge side of the graph.
    It opens no loop and yields a nullable value chunk, one value per input edge.
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_edges `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
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
* CrossProduct
*/
def CrossProduct : NLOp<"cross_product", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Cross every row of one loop's chunks with every row of another";

  let description = [{
    Given an outer chunk of N rows and an inner chunk of M rows it produces N*M
    rows: each outer column is block-repeated x M (outer row i becomes M
    consecutive rows) and each inner column is tiled x N (the whole inner chunk
    repeated N times), so result row k pairs outer row k/M with inner row k%M -
    every outer row against every inner row.

      %a, %b = nl.cross_product {%a} {%b}
                 : {!nl.chunk<!nl.node_id>} {!nl.chunk<!nl.node_id>}

    The two brace groups are the columns each factor contributes, in db.yield
    order; the op's results are the outer columns followed by the inner, each
    result chunk keeping its input's element type - only the row count changes.
    N and M are read from the first column of each group, so each group must be
    non-empty.
  }];

  // The outer and inner column groups to cross. Two variadic groups of
  // independent size need AttrSizedOperandSegments to record each group's size.
  let arguments = (ins Variadic<NLChunk>:$outer_columns,
                       Variadic<NLChunk>:$inner_columns);

  // One result per crossed column, outer group then inner group; inferred from
  // the operands since the broadcast keeps each chunk's element type.
  let results = (outs Variadic<NLChunk>:$results);

  // The two SSA brace groups mirror the two factors. The result types are
  // inferred, so only the operand chunk types are spelled - the same two
  // groups, each in braces, after the colon (as nl.output spells its chunk
  // types). The operandSegmentSizes attribute is populated and elided
  // automatically by the generated parser/printer.
  let assemblyFormat = [{
    `{` $outer_columns `}` `{` $inner_columns `}` attr-dict `:`
    `{` qualified(type($outer_columns)) `}` `{` qualified(type($inner_columns)) `}`
  }];
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
