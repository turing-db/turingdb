#ifndef TURINGDB_JOIN_OPS
#define TURINGDB_JOIN_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"

/**
* CrossProduct
*/
def CrossProduct : TuringOp<"cross_product", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRowSource", "getYieldedResultOffset"]>]> {
  let summary = "Cross every row of one column dataflow with every row of another";

  let description = [{
    Models a Cypher disconnected pattern, e.g. `MATCH (a)-->(b), (c)-->(d)`: two
    column dataflows that share no variable, whose result is the cartesian
    product of their rows. Each side is a self-contained db dataflow held in its
    own region - `leftFactor` and `rightFactor` - with no operands and no block
    arguments, so the two factors share no SSA value.

      %a, %b = db.cross_product factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %x : !db.column<!storage.node_id>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %y : !db.column<!storage.node_id>
      }

    Because the factors are disconnected, which columns end up in the result
    cannot be read off a data dependency (unlike get_out_edges' carry set within
    a connected traversal) - it must be stated. Each region ends with a db.yield
    naming exactly the columns that factor contributes, and the op's results are
    the left factor's yielded columns followed by the right factor's. Each factor
    must yield at least one column: a side's row count is read from its first
    yielded column, so a factor that surfaces none cannot be sized and the
    verifier rejects an empty db.yield.

    The result types are not spelled after the regions; the parser recovers them
    from the two yields and the verifier checks they line up. Reading the graph
    is deterministic, so the op is Pure.
  }];

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and creates the two empty factor
  // blocks; the caller fills each region and terminates it with a db.yield
  // whose operands match the result types contributed by that factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes)>
  ];
}

/**
* HashJoin
*/
def HashJoin : TuringOp<"hash_join", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRowSource", "getYieldedResultOffset"]>]> {
  let summary = "Cross two column dataflows, keeping the rows their key columns agree on";

  let description = [{
    The fused form of a db.cross_product whose product one db.filter cuts by a
    single db.eq between a column of each factor - an inner equi-join. It holds
    each side the way the product does, in its own region ending in a db.yield,
    and its results are the left factor's yielded columns followed by the right
    factor's.

      %a, %ka, %b, %kb = db.hash_join factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%x, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.yield %x, %k : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%y, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.yield %y, %k : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>
      } on 1, 1

    `on` names the key column of each side as an index into that factor's
    db.yield - the left factor's first, the right factor's second. A result row
    pairs a left row with a right row whose keys `=` holds equal, so the two keys
    need not share a type: 32 joins 32.0. A row whose key is null joins with
    nothing, which is what the equality it replaces did: `=` against a null gives
    null and the filter keeps only the true rows.

    The right factor is the built side and the left the probed one, so a left
    row's matches come out together and in right-factor order, however either
    side was chunked. That is not the order the product and its filter emitted
    the surviving rows in: the product re-runs its right factor once per chunk
    of the left, so a right side spanning several chunks interleaves them. Row
    order is what an ORDER BY is for, so the fusion is free to change it - but
    a LIMIT with no ORDER BY over a join can keep other rows than the same
    LIMIT over the product.

    As on db.cross_product the result types are not spelled after the regions;
    the parser recovers them from the two yields and the verifier checks they line
    up. Reading the graph is deterministic, so the op is Pure.
  }];

  // Which column of each factor's yield carries the join key: the left factor's,
  // then the right factor's.
  let arguments = (ins UI64Attr:$leftKey, UI64Attr:$rightKey);

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and the two key indices and creates
  // the two empty factor blocks; the caller fills each region and terminates it
  // with a db.yield whose operands match the result types contributed by that
  // factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "uint64_t":$leftKey, "uint64_t":$rightKey)>
  ];
}

/**
* Yield
*/
def Yield : TuringOp<"yield", [Pure, Terminator, ParentOneOf<["CrossProduct", "HashJoin", "Union", "Conditional", "ExplorePaths"]>]> {
  let summary = "Name the columns a region contributes to the result of the op holding it";

  let description = [{
    Terminates a factor region of a db.cross_product or a db.hash_join, naming
    the columns that factor contributes. Either op's results are the left
    factor's yielded columns followed by the right factor's. A factor must yield
    at least one column - the holder's verifier rejects an empty yield, since a
    side's row count is read from its first yielded column.

    It also terminates each branch of a db.union that has results and of a
    db.conditional, naming the rows that branch adds to them. It terminates each
    condition of a db.conditional and the hop region of a db.explore_paths too, yielding
    one boolean column: whether the row takes the branch, or which candidates of a hop
    pass its predicate.
  }];

  let arguments = (ins Variadic<Column>:$columns);

  // The yielded column types vary from query to query and cannot be built from
  // a recipe, so - when present - they are spelled after the colon, as in
  // db.output. The optional group, anchored on the operands, keeps the syntax
  // total: an empty yield (`db.yield`) still parses, then the holder's verifier
  // rejects it with a clear db-level diagnostic.
  let assemblyFormat = "( $columns^ `:` qualified(type($columns)) )? attr-dict";
}

#endif // TURINGDB_JOIN_OPS
