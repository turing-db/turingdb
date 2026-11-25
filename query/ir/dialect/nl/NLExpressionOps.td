#ifndef TURINGDB_NL_EXPRESSION_OPS
#define TURINGDB_NL_EXPRESSION_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

def Constant : NLOp<"constant", [Pure, ConstantLike, InferTypeOpAdaptor]> {
  let summary = "Materialize a constant scalar as a single-row chunk";
  let description = [{
      Represents a ColumnVector<T> with a single row
      %c = nl.constant(30 : i64) : !nl.chunk<i64>
  }];

  let arguments = (ins AnyAttrOf<[TypedAttrInterface, DenseF32ArrayAttr, ArrayAttr, DictionaryAttr]>: $value);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $value `)` attr-dict";
}

def BroadcastConstant : NLOp<"broadcast_constant", [Pure, RowAlignedThroughOperands]> {
  let summary = "Repeat a constant chunk's single value over the rows of a step";
  let description = [{
      A constant chunk holds one value standing for every row rather than one per
      row, so a step that walks rows - an aggregate's fold, a grouped fold - cannot
      read it: it would see the single row the constant is, not the rows the step
      has. This lays that value out over the rows of the driving relation, whose
      chunk the cardinality operand is, giving a row-aligned nullable value chunk
      the folds read like any column. The projection's sibling is nl.output's own
      cardinality operand, against which its constants broadcast as it emits.

      %r = nl.broadcast_constant %c, %n : (!nl.chunk<i64>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<i64>>

      The cardinality operand is absent when no relation drives the projection -
      RETURN 1 AS x, count(x) with no MATCH - which is one row of its own, so the
      value is laid out over that single row:

      %r = nl.broadcast_constant %c : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  // The cardinality is the only variable-length operand, so MLIR can tell it apart
  // from the value without operand-segment sizes (as nl.for's limit handle is).
  let arguments = (ins NLChunk:$value, Optional<NLChunk>:$cardinality);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$value (`,` $cardinality^)? attr-dict `:` functional-type(operands, results)";
}

/**
* RowConstant
*/
def RowConstant : NLOp<"row_constant", [Pure, ConstantResult, SameOperandsAndResultType]> {
  let summary = "Read the single row a computation over constants produced as a constant";
  let description = [{
      A row-wise op whose operands are all constants is computed once, ahead of the loops,
      over the single row its constants are laid out on. This reads that row as the constant
      it is, standing for every row of whichever step reads it:

      %from = nl.broadcast_constant %one : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
      %to = nl.broadcast_constant %thousand : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
      %row = nl.range %from, %to : (...) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
      %list = nl.row_constant %row : !nl.chunk<!storage.nullable<!storage.list<i64>>>

      A body run once per row reads each row of an nl.each_row step as a constant for the
      same reason, which is what db.row_constant lowers to:

      %rows = nl.each_row {%p} : {!nl.chunk<!storage.node_id>}
      nl.for %row in %rows : !nl.iter<!nl.chunk<!storage.node_id>> {
        %pc = nl.row_constant %row : !nl.chunk<!storage.node_id>
        ...
      }

      No row of any other step may reach the operand.
  }];

  let arguments = (ins NLChunk:$row);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$row attr-dict `:` type($row)";
}

/**
* Add
*/
def Add : NLOp<"add", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise addition of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double + double), and nullability (e.g. null + non-null = null).

      %r = nl.add %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Concat : NLOp<"concat", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise concatenation of two string or two list chunks";
  let description = [{
      %r = nl.concat %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.string>
      %l = nl.concat %s, %t : (!nl.chunk<!storage.list<none>>, !nl.chunk<!storage.list<none>>) -> !nl.chunk<!storage.list<none>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Sub : NLOp<"sub", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise subtraction of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double - double), and nullability (e.g. null - non-null = null).

      %r = nl.sub %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Mul : NLOp<"mul", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise multiplication of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double * double), and nullability (e.g. null * non-null = null).

      %r = nl.mul %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Div : NLOp<"div", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise division of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double / double), and nullability (e.g. null / non-null = null).
      Division by zero throws.

      %r = nl.div %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Mod : NLOp<"mod", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise modulo of two value chunks";
  let description = [{
      Modulo by zero throws.

      %r = nl.mod %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Pow : NLOp<"pow", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise exponentiation of two value chunks";
  let description = [{
      %r = nl.pow %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<f64>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Eq : NLOp<"eq", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise equality of two value chunks";
  let description = [{
      %r = nl.eq %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>

      Neither operand is the null literal; a test for a null is nl.is_null.
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";

  let hasVerifier = 1;
}

def Neq : NLOp<"neq", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise inequality of two value chunks";
  let description = [{
      %r = nl.neq %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";

  let hasVerifier = 1;
}

def IsNull : NLOp<"is_null", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether a chunk holds no value";
  let description = [{
      Imperative counterpart of db.is_null, over any chunk as it stands: an absent value, an
      invalid ID, the null path and a cell holding null are null, and the result never is.

      %r = nl.is_null %x : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.bool>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLBoolChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def IsNotNull : NLOp<"is_not_null", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether a chunk holds a value";
  let description = [{
      Imperative counterpart of db.is_not_null, the negation of nl.is_null.

      %r = nl.is_not_null %x : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLBoolChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def Gt : NLOp<"gt", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise greater-than comparison of two value chunks";
  let description = [{
      %r = nl.gt %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Lt : NLOp<"lt", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise less-than comparison of two value chunks";
  let description = [{
      %r = nl.lt %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Gte : NLOp<"gte", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise greater-than-or-equal comparison of two value chunks";
  let description = [{
      %r = nl.gte %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Lte : NLOp<"lte", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise less-than-or-equal comparison of two value chunks";
  let description = [{
      %r = nl.lte %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def StartsWith : NLOp<"starts_with", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value starts with the given prefix";
  let description = [{
      %r = nl.starts_with %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def EndsWith : NLOp<"ends_with", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value ends with the given suffix";
  let description = [{
      %r = nl.ends_with %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Contains : NLOp<"contains", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value contains the given substring";
  let description = [{
      %r = nl.contains %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def In : NLOp<"in", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each chunk value is an element of the given list";
  let description = [{
      %r = nl.in %x, %y : (!nl.chunk<i64>, !nl.chunk<!storage.list<i64>>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def IndexOf : NLOp<"index_of", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise position of the first element of a list equal to a value, or -1";
  let description = [{
      %r = nl.index_of %xs, %x : (!nl.chunk<!storage.list<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def And : NLOp<"and", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical AND of two boolean chunks";
  let description = [{
      Three-valued (Kleene) logic: null AND false is false, null AND true is
      null, so the result is nullable when either operand is.

      %r = nl.and %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Or : NLOp<"or", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical OR of two boolean chunks";
  let description = [{
      Three-valued (Kleene) logic: null OR true is true, null OR false is
      null, so the result is nullable when either operand is.

      %r = nl.or %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Xor : NLOp<"xor", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical XOR of two boolean chunks";
  let description = [{
      %r = nl.xor %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Not : NLOp<"not", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical NOT of a boolean chunk";
  let description = [{
      Three-valued logic: NOT null is null, so the result is nullable when the
      operand is.

      %r = nl.not %x : (!nl.chunk<i1>) -> !nl.chunk<!storage.bool>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def ToNullable : NLOp<"to_nullable", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Read a plain value chunk as a nullable value chunk holding a value in every row";
  let description = [{
      %r = nl.to_nullable %x : (!nl.chunk<ui64>) -> !nl.chunk<!storage.nullable<ui64>>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def CheckedCast : NLOp<"checked_cast", [NoMemoryEffect, RowAlignedThroughOperands]> {
  let summary = "Read the node or the relationship each tagged cell of a chunk holds";
  let description = [{
      db.checked_cast lowers to this. A null cell, and every row of a null literal, reads
      as the invalid ID; a cell holding another value raises.

      %p = nl.checked_cast %v : (!nl.chunk<!storage.any>) -> !nl.chunk<!storage.node_id>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def ToAny : NLOp<"to_any", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Read a chunk as values of type any";
  let description = [{
      The chunk a column takes where its rows hold values of different types, each row
      keeping the type it has: a scalar, an entity, a list or a map becomes a tagged cell,
      and a null an absent one. A scalar is read out of a nullable chunk, never a plain
      one. A list chunk read as lists of the type its elements join into - tagged cells,
      or the one type an empty list's none joins to - keeps the views it holds, whose
      elements are tagged cells already.

      %r = nl.to_any %x : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.any>
      %l = nl.to_any %y : (!nl.chunk<!storage.list<!storage.string>>)
                          -> !nl.chunk<!storage.list<!storage.any>>
      %e = nl.to_any %z : (!nl.chunk<!storage.list<none>>)
                          -> !nl.chunk<!storage.list<!storage.node_id>>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
  let hasVerifier = 1;
}

def Case : NLOp<"case", [Pure, RowAlignedThroughOperands, AttrSizedOperandSegments]> {
  let summary = "Row-wise selection of the first branch whose condition holds";
  let description = [{
      The lowered db.case: parallel condition and value chunks, an optional default, and
      a nullable value chunk holding the branch each row took - an entity chunk when the
      branches are nodes or edges, whose null is the invalid ID rather than an absent
      optional. Every operand carries rows - a constant among them was laid out over the
      driving relation during lowering - so the selection reads one cell of each per row
      it writes.

      %r = nl.case({%c0}, {%v0}, %d) : (!nl.chunk<i1>, !nl.chunk<!storage.nullable<i64>>,
                                        !nl.chunk<!storage.nullable<i64>>)
                                        -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins Variadic<NLChunk>:$conditions,
                       Variadic<NLChunk>:$values,
                       Optional<NLChunk>:$defaultValue);
  let results   = (outs NLChunk:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $conditions `}` `,` `{` $values `}` (`,` $defaultValue^)? `)`
    attr-dict `:` functional-type(operands, results)
  }];
}

def CaseMerge : NLOp<"case_merge", [Pure, RowAlignedWithFirstOperand, AttrSizedOperandSegments]> {
  let summary = "Row-wise selection of the first branch whose condition holds, out of branches computed only over the rows reaching them";
  let description = [{
      The lowered db.lazy_case. Its operands are cut down the way the CASE reads its rows:
      the first condition holds one row per row of the CASE, each later one a row per row
      no earlier condition held for, each value a row per row its condition held for, and
      the default a row per row no condition held for. The kernel walks the rows of the
      first condition, with a cursor into every other operand.

      %r = nl.case_merge({%c0, %c1}, {%v0, %v1}, %d) : (!nl.chunk<i1>, !nl.chunk<i1>,
                                                        !nl.chunk<!storage.nullable<i64>>,
                                                        !nl.chunk<!storage.nullable<i64>>,
                                                        !nl.chunk<!storage.nullable<i64>>)
                                                        -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins Variadic<NLChunk>:$conditions,
                       Variadic<NLChunk>:$values,
                       Optional<NLChunk>:$defaultValue);
  let results   = (outs NLChunk:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $conditions `}` `,` `{` $values `}` (`,` $defaultValue^)? `)`
    attr-dict `:` functional-type(operands, results)
  }];
}

def FillNulls : NLOp<"fill_nulls", [Pure, RowAlignedWithFirstOperand]> {
  let summary = "Row-wise fill of the null rows of a chunk, in order, from the rows of another";
  let description = [{
      The merge of a lowered db.coalesce. `null_rows` is the mask of the rows of `value`
      its argument left null, before the value was converted to the coalesce's chunk, and
      `fill` holds one row per row it marks, in their order. The result holds the row of
      `value` where the mask does not hold and the next row of `fill` where it does.
      `value`, `fill` and the result are of one type. The arguments of a coalesce are
      merged back to front: each one's null rows take the merge of the arguments behind
      it, which was computed over those rows alone.

      %r = nl.fill_nulls %v, %nulls, %f : (!nl.chunk<!storage.nullable<i64>>,
                                           !nl.chunk<!storage.bool>,
                                           !nl.chunk<!storage.nullable<i64>>)
                                           -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$value, NLChunk:$null_rows, NLChunk:$fill);
  let results   = (outs NLChunk:$result);

  let hasVerifier = 1;

  let assemblyFormat = "$value `,` $null_rows `,` $fill attr-dict `:` functional-type(operands, results)";
}

def IsNotTrue : NLOp<"is_not_true", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether a condition is false or null";
  let description = [{
      The rows a CASE condition lets through to the next branch.

      %r = nl.is_not_true %c : (!nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.bool>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

#endif // TURINGDB_NL_EXPRESSION_OPS
