#ifndef TURINGDB_EXPRESSION_OPS
#define TURINGDB_EXPRESSION_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "DBFunctionOps.td"

def ConstantOp : TuringOp<"constant", [Pure, ConstantLike, DeclareOpInterfaceMethods<InferTypeOpInterface>]> {
  let summary = "Produces a constant value in a column";

  // A typed attribute for a scalar, a dense f32 array for an embedding, an array of
  // per-element attributes for a list literal, or a dictionary for map literals.
  // Neither array carries a type of its own, so the column's element type is what
  // inferReturnTypes reads off them: an embedding for the dense array, the homogeneity
  // verdict over the elements for the list - which is why a list needs no separate op.
  let arguments = (ins AnyAttrOf<[TypedAttrInterface, DenseF32ArrayAttr, ArrayAttr, DictionaryAttr]>: $value);
  let results = (outs Column : $result);

  let hasFolder = 1;

  let assemblyFormat = "`(` $value `)` attr-dict";
}

def BroadcastConstant : TuringOp<"broadcast_constant", [Pure, AllTypesMatch<["value", "result"]>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Lay a constant column's single value out over the rows of a relation";
  let description = [{
    A constant column holds one value standing for every row rather than one per row,
    so nothing shaped by rows can read it: a filter has no row of it to drop, a dedup
    no two rows to tell apart, a count no rows to tally. This binds that value to the
    rows of the relation the driver column carries, giving a column that carries rows
    like any other:

      %p = db.scan_nodes() : !db.column<!storage.node_id>
      %c = db.constant(1 : i64) : !db.column<i64>
      %x = db.broadcast_constant(%c, %p) : (!db.column<i64>, !db.column<!storage.node_id>) -> !db.column<i64>

    It is what a WITH publishing constants alone emits: `MATCH (p:Person) WITH 1 AS x`
    publishes one row per Person, not the single row the literal is, so the statements
    after the barrier - a WHERE cutting those rows, a count(*) tallying them, a pattern
    crossed with them - read a column that has them.

    The driver is absent when no relation drives the scope, which is then the single row
    the constants are - `WITH 1 AS x WHERE x > 100`, whose one row the filter drops - so
    the value is laid out over that row:

      %x = db.broadcast_constant(%c) : (!db.column<i64>) -> !db.column<i64>

    The result holds the value it lays out, so it has the value's type.
  }];

  // The driver is the only optional operand, so MLIR tells it from the value without
  // operand-segment sizes, as nl.broadcast_constant's cardinality is.
  let arguments = (ins Column:$value, Optional<Column>:$driver);
  let results = (outs Column:$result);

  let assemblyFormat = [{
    `(` $value (`,` $driver^)? `)` attr-dict `:` functional-type(operands, results)
  }];
}

def RowConstant : TuringOp<"row_constant", [Pure, ConstantResult, SameOperandsAndResultType, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Read an argument of a body run once per row as a constant";
  let description = [{
    A db.call_subquery, db.exists_subquery, db.count_subquery or db.collect_subquery
    without `carries_scope` runs its body once per input row, so each argument of that
    body holds one value for the whole run. This reads such an argument as the constant
    it is, as Postgres reads a correlated parameter of a subplan: no op carries it
    through the body, and an op reading it per row has it laid out over its own rows.

      %r:2 = db.call_subquery(%p) {
      ^bb0(%pIn: !db.column<!storage.node_id>):
        %pc = db.row_constant %pIn : !db.column<!storage.node_id>
        %s, %e, %et, %i = db.get_out_edges(%pc, {}) : ...
        %name = db.get_node_properties(%pc, "name") : ...
        db.subquery_yield {%name, %i} : ...
      } : ...

    The operand is an argument of a body run once per row.
  }];

  let arguments = (ins Column:$row);
  let results = (outs Column:$result);

  let assemblyFormat = "$row attr-dict `:` type($row)";

  let hasVerifier = 1;
}

def ToNullable : TuringOp<"to_nullable", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Read a column as the nullable column of its values, holding a value in every row";
  let description = [{
    What a rewrite answering a nullable column with a plain one wraps it in, so that
    every reader of the column keeps the type it was built against:

      %l = db.to_nullable %n : (!db.column<ui64>) -> !db.column<!storage.nullable<ui64>>
  }];

  let arguments = (ins Column:$operand);
  let results   = (outs Column:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";

  let hasVerifier = 1;
}

def CheckedCast : TuringOp<"checked_cast", [NoMemoryEffect, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Read the node or the relationship each row's value holds";
  let description = [{
    A pattern naming a variable whose type is known per row - a value read out of a map, a
    mixed list or a CASE - reads the entity each row holds as a column of IDs:

      %p = db.checked_cast %v : (!db.column<!storage.any>) -> !db.column<!storage.node_id>

    Row r of the result is the node, or the relationship for an edge_id result, that row r's
    tagged cell holds. A null cell reads as the invalid ID an entity column spells its null
    with, from which a pattern matches nothing, and a cell holding any other value is the
    type error the row raises. The null literal reads as that invalid ID on every row, and
    a column already holding the entities reads as itself.

    It may raise, so it is computed over the rows reaching it: a constant operand is laid
    out over them first.
  }];

  let arguments = (ins Column:$operand);
  let results   = (outs Column:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";

  let hasVerifier = 1;
}

class PredicateOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, !listconcat(traits, [ConstantThroughOperands])>,
      Arguments<(ins Column:$lhs, Column:$rhs)> {
  let results = (outs ColumnBools:$result);
  let assemblyFormat = "$lhs`,` $rhs attr-dict `:` functional-type(operands, results)";
}

def AndOp : PredicateOp<"and", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Performs row-wise logical AND on its two input columns";
  let arguments = (ins Column : $lhs, Column : $rhs);

  let extraClassDeclaration = [{
    // Whether an and over these two columns raises on no row
    static bool isSpeculatableOver(::mlir::Value lhs, ::mlir::Value rhs);
  }];
}

def OrOp : PredicateOp<"or", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Performs row-wise logical OR on its two input columns";
  let arguments = (ins Column : $lhs, Column : $rhs);
}

def XorOp : PredicateOp<"xor", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Performs row-wise logical XOR on its two input boolean columns";
  let arguments = (ins Column : $lhs, Column : $rhs);
}

def NotOp : TuringOp<"not", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>, ConstantThroughOperands]> {
  let summary = "Row-wise logical NOT of a boolean column";
  let arguments = (ins Column:$operand);
  let results   = (outs ColumnBools:$result);
  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def EqOp : PredicateOp<"eq", [Pure]> {
  let summary = "Row-wise equality of two value columns";
  let description = [{
      Result is always an abstraction over a (possibly nullable) mask (bool vector).

      %r = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<bool>

      Neither operand is the null literal: a comparison with it is null on every row, which
      is the null itself, and a test for a null is db.is_null.
  }];

  let hasVerifier = 1;
}

def NeqOp : PredicateOp<"neq", [Pure]> {
  let summary = "Row-wise inequality of two value columns";

  let hasVerifier = 1;
}

def IsNullOp : TuringOp<"is_null", [Pure, ConstantThroughOperands]> {
  let summary = "Row-wise test of whether a column holds no value";
  let description = [{
      `x IS NULL`: true on the rows where the operand holds no value - an absent value, the
      invalid ID of an entity an OPTIONAL MATCH missed, the null path, a list or map cell
      holding null - and false on every other row. The result is never null.

      %r = db.is_null %x : (!db.column<!storage.nullable<i64>>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$operand);
  let results   = (outs ColumnBools:$result);
  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def IsNotNullOp : TuringOp<"is_not_null", [Pure, ConstantThroughOperands]> {
  let summary = "Row-wise test of whether a column holds a value";
  let description = [{
      `x IS NOT NULL`, the negation of db.is_null. The result is never null.

      %r = db.is_not_null %x : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$operand);
  let results   = (outs ColumnBools:$result);
  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

// An arithmetic operator may raise on a type-erased cell holding a value it does not compute
// over, on a zero divisor, or on an overflow: each op says which operands rule them out
class ArithmeticOp<string mnemonic>
    : BinaryOp<mnemonic, [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>, InferColumnType]> {
  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def AddOp : ArithmeticOp<"add"> {
  let summary = "Row-wise addition of two value columns";
}

def ConcatOp : BinaryOp<"concat", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>, InferColumnType]> {
  let summary = "Row-wise concatenation of two string or two list columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def IndexOf : BinaryOp<"index_of", [Pure]> {
  let summary = "Row-wise position of the first element of a list equal to a value";

  let description = [{
    `coll.indexOf(['a', 'b'], 'b')`

      %i = db.index_of %xs, %x : (!db.column<none>, !db.column<!storage.string>) -> !db.column<none>

    Row r of the result is the 0-based position of the first element of row r's list equal
    to row r's value, and -1 where no element is. It is null where the list or the value is
    null. A null element matches nothing, where db.in answers null over it.
  }];

  let arguments = (ins Column:$list, Column:$value);
  let results   = (outs Column:$result);

  let assemblyFormat = "$list `,` $value attr-dict `:` functional-type(operands, results)";
}

def SubOp : ArithmeticOp<"sub"> {
  let summary = "Row-wise subtraction of two value columns";
}

def MulOp : ArithmeticOp<"mul"> {
  let summary = "Row-wise multiplication of two value columns";
}

def DivOp : ArithmeticOp<"div"> {
  let summary = "Row-wise division of two value columns";
}

def ModOp : ArithmeticOp<"mod"> {
  let summary = "Row-wise modulo of two value columns";
}

def PowOp : ArithmeticOp<"pow"> {
  let summary = "Row-wise exponentiation of two value columns";
}

def GtOp : PredicateOp<"gt", [Pure]> {
  let summary = "Row-wise greater-than comparison of two value columns";
}

def LtOp : PredicateOp<"lt", [Pure]> {
  let summary = "Row-wise less-than comparison of two value columns";
}

def GteOp : PredicateOp<"gte", [Pure]> {
  let summary = "Row-wise greater-than-or-equal comparison of two value columns";
}

def LteOp : PredicateOp<"lte", [Pure]> {
  let summary = "Row-wise less-than-or-equal comparison of two value columns";
}

def StartsWithOp : PredicateOp<"starts_with", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Row-wise test of whether each string column value starts with the given prefix";
}

def EndsWithOp : PredicateOp<"ends_with", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Row-wise test of whether each string column value ends with the given suffix";
}

def ContainsOp : PredicateOp<"contains", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Row-wise test of whether each string column value contains the given substring";
}

def InOp : PredicateOp<"in", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Row-wise test of whether each value is an element of the given list";

  let description = [{
    A type-erased cell on the right is iterated as db.unwind iterates its source: the list
    it holds, or a list of one where it holds any other value.
  }];
}

def Case : TuringOp<"case", [Pure, ConstantThroughOperands, AttrSizedOperandSegments, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Row-wise selection of the first branch whose condition holds";

  let description = [{
    A selection between columns computed for every row.
    `conditions` and `values` are parallel: a row takes the value of the first branch
    whose condition is true, the default when none is, and null when there is no default.

      %r = db.case({%c0, %c1}, {%v0, %v1}, %d)
             : (!db.column<!storage.bool>, !db.column<!storage.bool>,
                !db.column<i64>, !db.column<i64>, !db.column<i64>) -> !db.column<none>

    A Cypher CASE is a db.lazy_case instead, and a coalesce() a db.coalesce, which compute
    a branch or an argument only over the rows that reach it.

    The result is the one element every value holds, nullable unless it holds its own
    null, the null literal holding whatever the others do. Nodes, edges and paths are
    selected into the column they already are, which carries the null of an unmatched row
    in a value of its own. A result of none is resolved during lowering, as for the
    arithmetic ops: the branches promote against each other into one nullable value
    column. A selection walks rows, so - unlike the arithmetic ops - it is not constant
    through its operands: the constants among them are laid out over the driving relation
    during lowering.
  }];

  let arguments = (ins Variadic<Column>:$conditions,
                       Variadic<Column>:$values,
                       Optional<Column>:$defaultValue);
  let results = (outs Column:$result);

  let extraClassDeclaration = [{
    // The value of every branch, then the default
    void collectSelectedValues(::llvm::SmallVectorImpl<::mlir::Value>& values);
  }];

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $conditions `}` `,` `{` $values `}` (`,` $defaultValue^)? `)`
    attr-dict `:` functional-type(operands, results)
  }];
}

def LazyCase : TuringOp<"lazy_case", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet", "forwardsColumns", "getForwardedColumnsMutable"]>]> {
  let summary = "A Cypher CASE, whose conditions and values are computed only over the rows reaching them";

  let description = [{
    The branches are regions rather than operands, so that what a row does not reach is
    never computed for it: `CASE WHEN t = 0 THEN 0 ELSE 10 / t END` divides by no zero.
    The regions alternate condition and value, one pair per WHEN, and end with the ELSE
    when there is one:

      %r = db.lazy_case({%t}) ({
        ^bb0(%tIn: !db.column<i64>):
          %c = db.eq %tIn, %zero : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
          db.case_yield %c : !db.column<!storage.bool>
      }, {
        ^bb0(%tIn: !db.column<i64>):
          db.case_yield %zero : !db.column<i64>
      }, {
        ^bb0(%tIn: !db.column<i64>):
          %q = db.div %ten, %tIn : (!db.column<i64>, !db.column<i64>) -> !db.column<none>
          db.case_yield %q : !db.column<none>
      }) : (!db.column<i64>) -> !db.column<none>

    Every region takes one argument per carried column: the columns of the rows in flight
    that some region reads, as db.list_comprehension carries them. A condition region sees
    the rows no earlier condition held for, a value region the rows its condition held
    for, and the ELSE the rows no condition held for. A row takes the first branch whose
    condition is true and null when none is and there is no ELSE. A constant needs no
    carrying.

    Every later region sees a subset of the rows a condition region sees, so a condition
    region may forward columns it computes through its db.case_yield. Each later region
    takes them as leading arguments, ahead of the carried ones, after those of every
    condition region before it:

      %r = db.lazy_case({%n, %french}) {
        ^bb0(%nIn: !db.column<!storage.node_id>, %frenchIn: !db.column<!storage.nullable<i1>>):
          db.case_yield %frenchIn : !db.column<!storage.nullable<i1>>
      }, {
        ^bb0(%nIn: !db.column<!storage.node_id>, %frenchIn: !db.column<!storage.nullable<i1>>):
          db.case_yield %one : !db.column<i64>
      }, {
        ^bb0(%nIn: !db.column<!storage.node_id>, %frenchIn: !db.column<!storage.nullable<i1>>):
          %l = db.get_node_label_set(%nIn) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
          %c = db.check_label_constraint(%l, ["Founder"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
          db.case_yield %c : !db.column<!storage.bool> forwarding(%l : !db.column<!storage.labelset_id>)
      }, {
        ^bb0(%lIn: !db.column<!storage.labelset_id>, %nIn: !db.column<!storage.node_id>, %frenchIn: !db.column<!storage.nullable<i1>>):
          db.case_yield %two : !db.column<i64>
      }, {
        ^bb0(%lIn: !db.column<!storage.labelset_id>, %nIn: !db.column<!storage.node_id>, %frenchIn: !db.column<!storage.nullable<i1>>):
          %e = db.check_label_constraint(%lIn, ["Exotic"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
          db.case_yield %e : !db.column<!storage.bool>
      } : (!db.column<!storage.node_id>, !db.column<!storage.nullable<i1>>) -> !db.column<none>

    The simple form `CASE s WHEN v THEN ...` is the generic one, with a db.eq of the
    subject against each WHEN value as the condition. The subject is computed once over
    every row and, unless it is a constant, carried as the last column.

    The result is typed as db.case's is, over the values the value regions yield.
  }];

  let arguments = (ins Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$result);

  let regions = (region VariadicRegion<SizedRegion<1>>:$branches);

  let extraClassDeclaration = [{
    // The value each WHEN's value region yields, then the ELSE's
    void collectSelectedValues(::llvm::SmallVectorImpl<::mlir::Value>& values);
  }];

  let hasVerifier = 1;
  let hasRegionVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $columns_to_filter `}` `)` $branches attr-dict `:` functional-type(operands, results)
  }];
}

def Coalesce : TuringOp<"coalesce", [RecursiveMemoryEffects, MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet", "forwardsColumns", "getForwardedColumnsMutable"]>]> {
  let summary = "A coalesce(), whose arguments are computed only over the rows every earlier one left null";

  let description = [{
    The arguments are regions rather than operands, one per argument in their order, so
    that what a row does not reach is never computed for it:
    `coalesce(CASE WHEN t = 0 THEN 0 END, 10 / t)` divides by no zero.

      %r = db.coalesce({%t}) {
        ^bb0(%tIn: !db.column<i64>):
          %z = db.lazy_case({%tIn}) { ... } : (!db.column<i64>) -> !db.column<!storage.nullable<i64>>
          db.case_yield %z : !db.column<!storage.nullable<i64>>
      }, {
        ^bb0(%tIn: !db.column<i64>):
          %q = db.div %ten, %tIn : (!db.column<i64>, !db.column<i64>) -> !db.column<none>
          db.case_yield %q : !db.column<none>
      } : (!db.column<i64>) -> !db.column<none>

    Every region takes one argument per carried column, as db.lazy_case's regions do. The
    first region sees every row, and each later one the rows every earlier region yielded
    null for. A row takes the first value that is not null, and null when every region
    yields null. Every region but the last may forward columns to the regions after it,
    as a condition region of db.lazy_case does.

    The result is typed as db.case's is, over the values the regions yield. The op has the
    effects of its regions, and raises where any op nested in them may, however deep: a
    Pure db.lazy_case reports no raise of the ops it holds. So one nothing reads stays
    where a row may raise on an argument it reaches.

    The canonical form holds no region yielding the null literal, but for one holding an
    op that may raise, and, where the result names its element, none behind a region
    yielding a literal that is not null. A coalesce left with one region is the value that
    region yields, where the value has the coalesce's type.
  }];

  let arguments = (ins Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$result);

  let regions = (region VariadicRegion<SizedRegion<1>>:$arguments);

  let extraClassDeclaration = [{
    // The value each region yields, in the order of the regions
    void collectSelectedValues(::llvm::SmallVectorImpl<::mlir::Value>& values);
  }];

  let hasVerifier = 1;
  let hasRegionVerifier = 1;
  let hasCanonicalizer = 1;

  let assemblyFormat = [{
    `(` `{` $columns_to_filter `}` `)` $arguments attr-dict `:` functional-type(operands, results)
  }];
}

def CaseYield : TuringOp<"case_yield", [Pure, Terminator, ParentOneOf<["LazyCase", "Coalesce"]>]> {
  let summary = "Name the condition or the value a region of a db.lazy_case or a db.coalesce computes, and the columns it forwards to the regions after it";

  let arguments = (ins Column:$value, Variadic<Column>:$forwarded);

  let builders = [
    OpBuilder<(ins "::mlir::Value":$value), [{
      build($_builder, $_state, value, ::mlir::ValueRange {});
    }]>
  ];

  let assemblyFormat = [{
    $value attr-dict `:` qualified(type($value)) (`forwarding` `(` $forwarded^ `:` qualified(type($forwarded)) `)`)?
  }];
}

#endif // TURINGDB_EXPRESSION_OPS
