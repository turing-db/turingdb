#ifndef TURINGDB_FUNCTION_OPS
#define TURINGDB_FUNCTION_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/Interfaces/InferTypeOpInterface.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageEnums.td"

// The column a function or an operator computes holds what its element rule answers over the
// columns its operands declare (DBColumnElement), inferred when the op is built
def InferColumnType : TraitList<[
    DeclareOpInterfaceMethods<InferTypeOpInterface>,
    NativeOpTrait<"InferColumnType", [], [{
      static bool isCompatibleReturnTypes(::mlir::TypeRange inferred, ::mlir::TypeRange declared);
    }], [{
      ::llvm::LogicalResult $cppClass::inferReturnTypes(::mlir::MLIRContext* context,
                                                        ::std::optional<::mlir::Location> location,
                                                        ::mlir::ValueRange operands,
                                                        ::mlir::DictionaryAttr attributes,
                                                        ::mlir::PropertyRef properties,
                                                        ::mlir::RegionRange regions,
                                                        ::llvm::SmallVectorImpl<::mlir::Type>& inferredReturnTypes) {
        inferredReturnTypes.push_back(::db::inferColumnType(context, getOperationName(), operands));
        return ::mlir::success();
      }

      bool $cppClass::isCompatibleReturnTypes(::mlir::TypeRange inferred, ::mlir::TypeRange declared) {
        return ::db::areCompatibleColumnTypes(inferred, declared);
      }
    }]>
]>;

class UnaryFunctionOp<string mnemonic, list<Trait> traits>
    : TuringOp<mnemonic, !listconcat(traits, [ConstantThroughOperands, InferColumnType])> {
  let summary = "Apply a unary function row-wise to a value column";

  let arguments = (ins Column:$input);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
  }];
}

// A unary function reading values of some types raises on a row holding another, which a
// column of type-erased cells or of an unresolved type can: it is speculatable over a
// column of the types it reads alone
class TypedUnaryFunctionOp<string mnemonic, string readTypes, list<Trait> traits = []>
    : UnaryFunctionOp<mnemonic, !listconcat([MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>], traits)> {
  let extraClassDefinition = [{
    ::mlir::Speculation::Speculatability $cppClass::getSpeculatability() {
      return speculatableOverElement<}] # readTypes # [{>(getInput());
    }
  }];
}

// A conversion raises on a type-erased cell holding a value it has no rule for. The analyzer
// turns away a column of one type it has no rule for, so only such a cell can raise
class ConversionFunctionOp<string mnemonic>
    : UnaryFunctionOp<mnemonic, [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let extraClassDefinition = [{
    ::mlir::Speculation::Speculatability $cppClass::getSpeculatability() {
      return speculatableOverKnownTypes(getInput());
    }
  }];
}

class NumberFunctionOp<string mnemonic> : TypedUnaryFunctionOp<mnemonic, "::mlir::IntegerType, ::mlir::FloatType">;
class TextFunctionOp<string mnemonic> : TypedUnaryFunctionOp<mnemonic, "::mlir::storage::StringType">;
class ListFunctionOp<string mnemonic> : TypedUnaryFunctionOp<mnemonic, "::mlir::storage::ListType">;

def Labels : TypedUnaryFunctionOp<"labels", "::mlir::storage::NodeIDType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;
def EdgeType : TypedUnaryFunctionOp<"edge_type", "::mlir::storage::EdgeIDType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;
def Keys : TypedUnaryFunctionOp<"keys", "::mlir::storage::NodeIDType, ::mlir::storage::EdgeIDType, ::mlir::storage::MapType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;
def PropertyMap : TypedUnaryFunctionOp<"properties", "::mlir::storage::NodeIDType, ::mlir::storage::EdgeIDType, ::mlir::storage::MapType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;

// The two ends of each edge of a column, as node columns of their own. The edge is read as
// the graph stores it, so a hop that walked it backwards or undirected reports the same
// pair rather than the one its own direction suggests.
def StartNode : TypedUnaryFunctionOp<"start_node", "::mlir::storage::EdgeIDType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;
def EndNode : TypedUnaryFunctionOp<"end_node", "::mlir::storage::EdgeIDType", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;

def ToInteger : ConversionFunctionOp<"to_integer">;
def ToFloat : ConversionFunctionOp<"to_float">;
def ToBoolean : ConversionFunctionOp<"to_boolean">;
def ToDateTime : TypedUnaryFunctionOp<"to_datetime", "::mlir::storage::StringType">;
def ToDuration : TypedUnaryFunctionOp<"to_duration", "::mlir::IntegerType">;

// Any map can name an invalid point - a latitude past 90, an unknown crs - so it is never
// speculatable
def ToPoint : UnaryFunctionOp<"to_point", [MayRaise]>;
def ToString : ConversionFunctionOp<"to_string">;

def ToIntegerOrNull : UnaryFunctionOp<"to_integer_or_null", [Pure]>;
def ToFloatOrNull : UnaryFunctionOp<"to_float_or_null", [Pure]>;
def ToBooleanOrNull : UnaryFunctionOp<"to_boolean_or_null", [Pure]>;
def ToStringOrNull : UnaryFunctionOp<"to_string_or_null", [Pure]>;

// Each element converts as the OrNull conversion converts it, so one with no rule is null
def ToIntegerList : ListFunctionOp<"to_integer_list">;
def ToFloatList : ListFunctionOp<"to_float_list">;
def ToBooleanList : ListFunctionOp<"to_boolean_list">;
def ToStringList : ListFunctionOp<"to_string_list">;

// The calendar fields of an instant, as integer columns. The sub-second pair reads the
// fraction of the second truncated to its own precision, so .123456 of a second in is a
// db.datetime_millisecond of 123 and a db.datetime_microsecond of 123456.
def DateTimeYear : TypedUnaryFunctionOp<"datetime_year", "::mlir::storage::DateTimeType">;
def DateTimeMonth : TypedUnaryFunctionOp<"datetime_month", "::mlir::storage::DateTimeType">;
def DateTimeDay : TypedUnaryFunctionOp<"datetime_day", "::mlir::storage::DateTimeType">;
def DateTimeHour : TypedUnaryFunctionOp<"datetime_hour", "::mlir::storage::DateTimeType">;
def DateTimeMinute : TypedUnaryFunctionOp<"datetime_minute", "::mlir::storage::DateTimeType">;
def DateTimeSecond : TypedUnaryFunctionOp<"datetime_second", "::mlir::storage::DateTimeType">;
def DateTimeMillisecond : TypedUnaryFunctionOp<"datetime_millisecond", "::mlir::storage::DateTimeType">;
def DateTimeMicrosecond : TypedUnaryFunctionOp<"datetime_microsecond", "::mlir::storage::DateTimeType">;

// The units of a duration, each the whole count of that unit it spans, as integer columns.
def DurationYears : TypedUnaryFunctionOp<"duration_years", "::mlir::storage::DurationType">;
def DurationQuarters : TypedUnaryFunctionOp<"duration_quarters", "::mlir::storage::DurationType">;
def DurationMonths : TypedUnaryFunctionOp<"duration_months", "::mlir::storage::DurationType">;
def DurationWeeks : TypedUnaryFunctionOp<"duration_weeks", "::mlir::storage::DurationType">;
def DurationDays : TypedUnaryFunctionOp<"duration_days", "::mlir::storage::DurationType">;
def DurationHours : TypedUnaryFunctionOp<"duration_hours", "::mlir::storage::DurationType">;
def DurationMinutes : TypedUnaryFunctionOp<"duration_minutes", "::mlir::storage::DurationType">;
def DurationSeconds : TypedUnaryFunctionOp<"duration_seconds", "::mlir::storage::DurationType">;
def DurationMilliseconds : TypedUnaryFunctionOp<"duration_milliseconds", "::mlir::storage::DurationType">;
def DurationMicroseconds : TypedUnaryFunctionOp<"duration_microseconds", "::mlir::storage::DurationType">;
def DurationQuartersOfYear : TypedUnaryFunctionOp<"duration_quarters_of_year", "::mlir::storage::DurationType">;
def DurationMonthsOfYear : TypedUnaryFunctionOp<"duration_months_of_year", "::mlir::storage::DurationType">;
def DurationMonthsOfQuarter : TypedUnaryFunctionOp<"duration_months_of_quarter", "::mlir::storage::DurationType">;
def DurationDaysOfWeek : TypedUnaryFunctionOp<"duration_days_of_week", "::mlir::storage::DurationType">;
def DurationMinutesOfHour : TypedUnaryFunctionOp<"duration_minutes_of_hour", "::mlir::storage::DurationType">;
def DurationSecondsOfMinute : TypedUnaryFunctionOp<"duration_seconds_of_minute", "::mlir::storage::DurationType">;
def DurationMillisecondsOfSecond : TypedUnaryFunctionOp<"duration_milliseconds_of_second", "::mlir::storage::DurationType">;
def DurationMicrosecondsOfSecond : TypedUnaryFunctionOp<"duration_microseconds_of_second", "::mlir::storage::DurationType">;

// The fields of a point: its coordinates as float columns, the name of its CRS as a string
// column and its SRID as an integer column. A 2D point has no z or height and a cartesian
// one no longitude, latitude or height, so those four raise over a column of points too.
def PointX : TypedUnaryFunctionOp<"point_x", "::mlir::storage::PointType">;
def PointY : TypedUnaryFunctionOp<"point_y", "::mlir::storage::PointType">;
def PointZ : UnaryFunctionOp<"point_z", [MayRaise]>;
def PointLongitude : UnaryFunctionOp<"point_longitude", [MayRaise]>;
def PointLatitude : UnaryFunctionOp<"point_latitude", [MayRaise]>;
def PointHeight : UnaryFunctionOp<"point_height", [MayRaise]>;
def PointCRSName : TypedUnaryFunctionOp<"point_crs", "::mlir::storage::PointType">;
def PointSRID : TypedUnaryFunctionOp<"point_srid", "::mlir::storage::PointType">;

def CurrentDateTime : TuringOp<"current_datetime", [Pure, ConstantThroughOperands]> {
  let summary = "The instant the query runs, as a constant column";
  let description = [{
      What datetime() emits. The instant is read when the program is translated rather
      than carried in an attribute, so the same query prints the same IR on every run.
      Reading no operand makes it constant through them, which is what lays one instant
      over every row instead of one per row.

        %now = db.current_datetime() : !db.column<!storage.datetime>
  }];

  let arguments = (ins);
  let results   = (outs Column:$result);

  let assemblyFormat = "`(` `)` attr-dict `:` type($result)";
}

// The list family. Each reads a whole !storage.list cell and answers per row: db.size the
// element count as an integer, db.tail the same list one element shorter - a subspan of
// the cell, so the row still costs O(1) - and db.head and db.last the list's first and
// final element. A stored list may mix types, so those two answer with the type-erased
// !storage.any every element of a heterogeneous list rides, and an empty list
// reads as the null such a cell carries itself rather than as a nullable column of its own.
// The ID of a node, an edge, or the entity a type-erased cell holds, as an integer.
def ElementID : TypedUnaryFunctionOp<"element_id", "::mlir::storage::NodeIDType, ::mlir::storage::EdgeIDType">;

def EntityIDOf : TuringOp<"entity_id_of", [Pure, ConstantThroughOperands]> {
  let summary = "The node or the edge each value names by its ID";

  let description = [{
    The converse of db.element_id: for each row, the entity e of the result's kind - a node
    or an edge - for which `id(e) = value` holds, or null where there is none.

      %m = db.entity_id_of(%x) : (!db.column<!storage.any>) -> !db.column<!storage.node_id>
      %r = db.entity_id_of(%x) : (!db.column<i64>) -> !db.column<!storage.edge_id>

    A number names the entity whose ID it equals: an integer, or a double holding one, in
    range and not negative, so 3 and 3.0 name node 3, or edge 3. A node or an edge names the
    entity of the result's kind whose ID is its own number, since `=` compares an integer to
    either by its ID: a node names itself, and an edge the node of its number. Any other
    value - a null, a string, a boolean, a list, a map, a fractional or negative number, or
    a node or an edge a type-erased cell holds, which `=` tells from a number - names none,
    which the result column spells as its own null, the invalid ID. Whether the graph holds
    the entity is for the op reading the column to decide, as db.fetch_nodes does.

    It is what canonicalize_id_equalities compares a node or an edge to in place of its
    id(): `WHERE id(n) = x` becomes `WHERE n = entity_id_of(x)`, which every lookup by ID
    reads. It cannot raise, so it holds one value for the run where its operand does.

    A path has no ID, and the verifier rejects it.
  }];

  let arguments = (ins Column:$input);
  let results   = (outs AnyTypeOf<[ColumnNodeIDs, ColumnEdgeIDs]>:$result);

  let hasVerifier = 1;
  let hasFolder = 1;

  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Size : TypedUnaryFunctionOp<"size", "::mlir::storage::StringType, ::mlir::storage::ListType">;
def IsEmpty : TypedUnaryFunctionOp<"is_empty", "::mlir::storage::StringType, ::mlir::storage::ListType, ::mlir::storage::MapType">;
def CharLength : TextFunctionOp<"char_length">;
def Head : ListFunctionOp<"head">;
def Last : ListFunctionOp<"last">;
def Tail : ListFunctionOp<"tail">;

def Abs : NumberFunctionOp<"abs">;

// Unary minus: the number or the duration of the opposite sign. The opposite of the lowest
// integer or duration overflows, so it is speculatable over doubles alone.
def Negate : TypedUnaryFunctionOp<"negate", "::mlir::FloatType">;

// Unary plus: the number or the duration itself, and a type error on any other value
def UnaryPlus : TypedUnaryFunctionOp<"unary_plus", "::mlir::IntegerType, ::mlir::FloatType, ::mlir::storage::DurationType">;
def Sign : NumberFunctionOp<"sign">;
def Ceil : NumberFunctionOp<"ceil">;
def Floor : NumberFunctionOp<"floor">;
def Round : NumberFunctionOp<"round">;
def Sqrt : NumberFunctionOp<"sqrt">;
def Exp : NumberFunctionOp<"exp">;
def Log : NumberFunctionOp<"log">;
def Log10 : NumberFunctionOp<"log10">;
def Sin : NumberFunctionOp<"sin">;
def Cos : NumberFunctionOp<"cos">;
def Tan : NumberFunctionOp<"tan">;
def Cot : NumberFunctionOp<"cot">;
def Asin : NumberFunctionOp<"asin">;
def Acos : NumberFunctionOp<"acos">;
def Atan : NumberFunctionOp<"atan">;
def Degrees : NumberFunctionOp<"degrees">;
def Radians : NumberFunctionOp<"radians">;
def Haversin : NumberFunctionOp<"haversin">;

def ToUpper : TextFunctionOp<"to_upper">;
def ToLower : TextFunctionOp<"to_lower">;
def Trim : TextFunctionOp<"trim">;
def LTrim : TextFunctionOp<"ltrim">;
def RTrim : TextFunctionOp<"rtrim">;
def Reverse : TypedUnaryFunctionOp<"reverse", "::mlir::storage::StringType, ::mlir::storage::ListType">;

class NormalFormFunctionOp<string mnemonic> : TextFunctionOp<mnemonic> {
  let arguments = (ins Column:$input, UnicodeNormalForm:$form);

  let assemblyFormat = [{
    `(` $input `,` $form `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Normalize : NormalFormFunctionOp<"normalize">;
def IsNormalized : NormalFormFunctionOp<"is_normalized">;

def Substring : TuringOp<"substring", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Take the run of each row's string its start and length span";

  let description = [{
      %s = db.substring(%name, %from, %count) : (!db.column<!storage.string>, !db.column<i64>,
                                                 !db.column<i64>) -> !db.column<!storage.string>
  }];

  let arguments = (ins Column:$string, Column:$start, Optional<Column>:$length);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $string `,` $start (`,` $length^)? `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Split : TuringOp<"split", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Split on a delimiter, or on every delimiter of a list";

  let description = [{
      %parts = db.split(%name, %delimiter) : (!db.column<!storage.string>, !db.column<!storage.string>)
                                           -> !db.column<!storage.list<!storage.string>>
      %parts = db.split(%name, %delimiters) : (!db.column<!storage.string>,
                                               !db.column<!storage.list<!storage.string>>)
                                            -> !db.column<!storage.list<!storage.string>>
  }];

  let arguments = (ins Column:$string, Column:$delimiter);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $string `,` $delimiter `)` attr-dict `:` functional-type(operands, results)
  }];
}

class StringEndOp<string mnemonic> : TuringOp<mnemonic, [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Take the run of characters at one end of each row's string";

  let description = [{
      %s = db.left(%name, %count) : (!db.column<!storage.string>, !db.column<i64>) -> !db.column<!storage.string>
  }];

  let arguments = (ins Column:$string, Column:$length);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $string `,` $length `)` attr-dict `:` functional-type(operands, results)
  }];

  let extraClassDefinition = [{
    ::mlir::LogicalResult $cppClass::verify() {
      return verifyStringEnd(getOperation(), getString(), getLength(), getResult());
    }

    ::mlir::Speculation::Speculatability $cppClass::getSpeculatability() {
      return speculatableOverCharacters(getOperation());
    }
  }];
}

def Left : StringEndOp<"left">;
def Right : StringEndOp<"right">;

def Replace : TuringOp<"replace", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Replace every occurrence of a search string in each row's string";

  let description = [{
      %s = db.replace(%name, %search, %replacement) : (!db.column<!storage.string>, !db.column<!storage.string>,
                                                       !db.column<!storage.string>) -> !db.column<!storage.string>
  }];

  let arguments = (ins Column:$string, Column:$search, Column:$replacement);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $string `,` $search `,` $replacement `)` attr-dict `:` functional-type(operands, results)
  }];
}

def PointDistance : TuringOp<"point_distance", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "The distance between the points of each row, null for two points in different CRSs";

  let description = [{
      Euclidean between two cartesian points, and great-circle metres between two
      geographic ones. It raises on a tagged cell holding no point.

      %d = db.point_distance(%a, %b) : (!db.column<!storage.point>, !db.column<!storage.point>)
                                       -> !db.column<f64>
  }];

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $lhs `,` $rhs `)` attr-dict `:` functional-type(operands, results)
  }];
}

def PointWithinBBox : TuringOp<"point_within_bbox", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Whether each row's point lies in its box, null unless the three share a CRS";

  let description = [{
      The box is bounded by its lower left and upper right corners, both included. It
      raises on a tagged cell holding no point.

      %in = db.point_within_bbox(%p, %lowerLeft, %upperRight)
          : (!db.column<!storage.point>, !db.column<!storage.point>, !db.column<!storage.point>)
            -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$point, Column:$lowerLeft, Column:$upperRight);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $point `,` $lowerLeft `,` $upperRight `)` attr-dict `:` functional-type(operands, results)
  }];
}

class BinaryFunctionOp<string mnemonic, list<Trait> traits = []> : TuringOp<mnemonic, !listconcat([Pure, ConstantThroughOperands], traits)> {
  let summary = "Apply a binary function row-wise to two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $lhs `,` $rhs `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CosineSimilarity : BinaryFunctionOp<"cosine_similarity", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;
def EuclideanDistance : BinaryFunctionOp<"euclidean_distance", [DeclareOpInterfaceMethods<RowCostOpInterface>]>;

#endif // TURINGDB_FUNCTION_OPS
