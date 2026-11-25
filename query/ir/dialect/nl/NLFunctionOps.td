#ifndef TURINGDB_NL_FUNCTION_OPS
#define TURINGDB_NL_FUNCTION_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageEnums.td"

class NLUnaryFunctionOp<string mnemonic, list<Trait> extraTraits = []>
    : NLOp<mnemonic, !listconcat([Pure, RowAlignedThroughOperands], extraTraits)> {
  let summary = "Apply a unary scalar function row-wise to a value chunk";

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def Labels : NLUnaryFunctionOp<"labels">;
def EdgeType : NLUnaryFunctionOp<"edge_type">;
def Keys : NLUnaryFunctionOp<"keys", [ConstantThroughOperands]>;
def PropertyMap : NLUnaryFunctionOp<"properties", [ConstantThroughOperands]>;

// db.start_node and db.end_node lower to these: an edge chunk in, the node chunk holding
// the end of each of its edges out.
def StartNode : NLUnaryFunctionOp<"start_node">;
def EndNode : NLUnaryFunctionOp<"end_node">;

def ToInteger : NLUnaryFunctionOp<"to_integer", [ConstantThroughOperands]>;
def ToFloat : NLUnaryFunctionOp<"to_float", [ConstantThroughOperands]>;
def ToBoolean : NLUnaryFunctionOp<"to_boolean", [ConstantThroughOperands]>;
def ToDateTime : NLUnaryFunctionOp<"to_datetime", [ConstantThroughOperands]>;
def ToDuration : NLUnaryFunctionOp<"to_duration", [ConstantThroughOperands]>;
def ToPoint : NLUnaryFunctionOp<"to_point", [ConstantThroughOperands]>;
def ToString : NLUnaryFunctionOp<"to_string", [ConstantThroughOperands]>;

def ToIntegerOrNull : NLUnaryFunctionOp<"to_integer_or_null", [ConstantThroughOperands]>;
def ToFloatOrNull : NLUnaryFunctionOp<"to_float_or_null", [ConstantThroughOperands]>;
def ToBooleanOrNull : NLUnaryFunctionOp<"to_boolean_or_null", [ConstantThroughOperands]>;
def ToStringOrNull : NLUnaryFunctionOp<"to_string_or_null", [ConstantThroughOperands]>;

def ToIntegerList : NLUnaryFunctionOp<"to_integer_list", [ConstantThroughOperands]>;
def ToFloatList : NLUnaryFunctionOp<"to_float_list", [ConstantThroughOperands]>;
def ToBooleanList : NLUnaryFunctionOp<"to_boolean_list", [ConstantThroughOperands]>;
def ToStringList : NLUnaryFunctionOp<"to_string_list", [ConstantThroughOperands]>;

// The db.datetime_* family lowers to these. Each reads one instant and answers one
// integer, so a constant instant - what datetime() and datetime('...') are - keeps its
// constancy through the field read.
def DateTimeYear : NLUnaryFunctionOp<"datetime_year", [ConstantThroughOperands]>;
def DateTimeMonth : NLUnaryFunctionOp<"datetime_month", [ConstantThroughOperands]>;
def DateTimeDay : NLUnaryFunctionOp<"datetime_day", [ConstantThroughOperands]>;
def DateTimeHour : NLUnaryFunctionOp<"datetime_hour", [ConstantThroughOperands]>;
def DateTimeMinute : NLUnaryFunctionOp<"datetime_minute", [ConstantThroughOperands]>;
def DateTimeSecond : NLUnaryFunctionOp<"datetime_second", [ConstantThroughOperands]>;
def DateTimeMillisecond : NLUnaryFunctionOp<"datetime_millisecond", [ConstantThroughOperands]>;
def DateTimeMicrosecond : NLUnaryFunctionOp<"datetime_microsecond", [ConstantThroughOperands]>;

// The db.duration_* family lowers to these.
def DurationYears : NLUnaryFunctionOp<"duration_years", [ConstantThroughOperands]>;
def DurationQuarters : NLUnaryFunctionOp<"duration_quarters", [ConstantThroughOperands]>;
def DurationMonths : NLUnaryFunctionOp<"duration_months", [ConstantThroughOperands]>;
def DurationWeeks : NLUnaryFunctionOp<"duration_weeks", [ConstantThroughOperands]>;
def DurationDays : NLUnaryFunctionOp<"duration_days", [ConstantThroughOperands]>;
def DurationHours : NLUnaryFunctionOp<"duration_hours", [ConstantThroughOperands]>;
def DurationMinutes : NLUnaryFunctionOp<"duration_minutes", [ConstantThroughOperands]>;
def DurationSeconds : NLUnaryFunctionOp<"duration_seconds", [ConstantThroughOperands]>;
def DurationMilliseconds : NLUnaryFunctionOp<"duration_milliseconds", [ConstantThroughOperands]>;
def DurationMicroseconds : NLUnaryFunctionOp<"duration_microseconds", [ConstantThroughOperands]>;
def DurationQuartersOfYear : NLUnaryFunctionOp<"duration_quarters_of_year", [ConstantThroughOperands]>;
def DurationMonthsOfYear : NLUnaryFunctionOp<"duration_months_of_year", [ConstantThroughOperands]>;
def DurationMonthsOfQuarter : NLUnaryFunctionOp<"duration_months_of_quarter", [ConstantThroughOperands]>;
def DurationDaysOfWeek : NLUnaryFunctionOp<"duration_days_of_week", [ConstantThroughOperands]>;
def DurationMinutesOfHour : NLUnaryFunctionOp<"duration_minutes_of_hour", [ConstantThroughOperands]>;
def DurationSecondsOfMinute : NLUnaryFunctionOp<"duration_seconds_of_minute", [ConstantThroughOperands]>;
def DurationMillisecondsOfSecond : NLUnaryFunctionOp<"duration_milliseconds_of_second", [ConstantThroughOperands]>;
def DurationMicrosecondsOfSecond : NLUnaryFunctionOp<"duration_microseconds_of_second", [ConstantThroughOperands]>;

def PointX : NLUnaryFunctionOp<"point_x", [ConstantThroughOperands]>;
def PointY : NLUnaryFunctionOp<"point_y", [ConstantThroughOperands]>;
def PointZ : NLUnaryFunctionOp<"point_z", [ConstantThroughOperands]>;
def PointLongitude : NLUnaryFunctionOp<"point_longitude", [ConstantThroughOperands]>;
def PointLatitude : NLUnaryFunctionOp<"point_latitude", [ConstantThroughOperands]>;
def PointHeight : NLUnaryFunctionOp<"point_height", [ConstantThroughOperands]>;
def PointCRSName : NLUnaryFunctionOp<"point_crs", [ConstantThroughOperands]>;
def PointSRID : NLUnaryFunctionOp<"point_srid", [ConstantThroughOperands]>;

def CurrentDateTime : NLOp<"current_datetime", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "The instant the query runs, as a single-value chunk";
  let description = [{
      db.current_datetime lowers to this. The translator reads the clock and fills the
      chunk with the answer, so the op runs no step of its own.

        %now = nl.current_datetime() : !nl.chunk<!storage.datetime>
  }];

  let arguments = (ins);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` `)` attr-dict `:` type($result)";
}

// The list family db.size / db.head / db.last / db.tail lower to. A literal list is one
// cell standing for every row, and so is what these read off it, so each carries its
// operand's constancy rather than being spread over the rows it was read on.
// db.element_id lowers to this, and carries its operand's constancy as the list family does.
def ElementID : NLUnaryFunctionOp<"element_id", [ConstantThroughOperands]>;

// db.entity_id_of lowers to this: the node or edge chunk naming, row for row, the entity each
// value of its operand names by its ID, the invalid ID where it names none
def EntityIDOf : NLUnaryFunctionOp<"entity_id_of", [ConstantThroughOperands]> {
  let hasVerifier = 1;
}

def Size : NLUnaryFunctionOp<"size", [ConstantThroughOperands]>;
def IsEmpty : NLUnaryFunctionOp<"is_empty", [ConstantThroughOperands]>;
def CharLength : NLUnaryFunctionOp<"char_length", [ConstantThroughOperands]>;
def Head : NLUnaryFunctionOp<"head", [ConstantThroughOperands]>;
def Last : NLUnaryFunctionOp<"last", [ConstantThroughOperands]>;
def Tail : NLUnaryFunctionOp<"tail", [ConstantThroughOperands]>;

def Abs : NLUnaryFunctionOp<"abs", [ConstantThroughOperands]>;
def Negate : NLUnaryFunctionOp<"negate", [ConstantThroughOperands]>;
def UnaryPlus : NLUnaryFunctionOp<"unary_plus", [ConstantThroughOperands]>;
def Sign : NLUnaryFunctionOp<"sign", [ConstantThroughOperands]>;
def Ceil : NLUnaryFunctionOp<"ceil", [ConstantThroughOperands]>;
def Floor : NLUnaryFunctionOp<"floor", [ConstantThroughOperands]>;
def Round : NLUnaryFunctionOp<"round", [ConstantThroughOperands]>;
def Sqrt : NLUnaryFunctionOp<"sqrt", [ConstantThroughOperands]>;
def Exp : NLUnaryFunctionOp<"exp", [ConstantThroughOperands]>;
def Log : NLUnaryFunctionOp<"log", [ConstantThroughOperands]>;
def Log10 : NLUnaryFunctionOp<"log10", [ConstantThroughOperands]>;
def Sin : NLUnaryFunctionOp<"sin", [ConstantThroughOperands]>;
def Cos : NLUnaryFunctionOp<"cos", [ConstantThroughOperands]>;
def Tan : NLUnaryFunctionOp<"tan", [ConstantThroughOperands]>;
def Cot : NLUnaryFunctionOp<"cot", [ConstantThroughOperands]>;
def Asin : NLUnaryFunctionOp<"asin", [ConstantThroughOperands]>;
def Acos : NLUnaryFunctionOp<"acos", [ConstantThroughOperands]>;
def Atan : NLUnaryFunctionOp<"atan", [ConstantThroughOperands]>;
def Degrees : NLUnaryFunctionOp<"degrees", [ConstantThroughOperands]>;
def Radians : NLUnaryFunctionOp<"radians", [ConstantThroughOperands]>;
def Haversin : NLUnaryFunctionOp<"haversin", [ConstantThroughOperands]>;

def ToUpper : NLUnaryFunctionOp<"to_upper", [ConstantThroughOperands]>;
def ToLower : NLUnaryFunctionOp<"to_lower", [ConstantThroughOperands]>;
def Trim : NLUnaryFunctionOp<"trim", [ConstantThroughOperands]>;
def LTrim : NLUnaryFunctionOp<"ltrim", [ConstantThroughOperands]>;
def RTrim : NLUnaryFunctionOp<"rtrim", [ConstantThroughOperands]>;
def Reverse : NLUnaryFunctionOp<"reverse", [ConstantThroughOperands]>;

class NLNormalFormFunctionOp<string mnemonic> : NLUnaryFunctionOp<mnemonic, [ConstantThroughOperands]> {
  let arguments = (ins NLChunk:$operand, UnicodeNormalForm:$form);

  let assemblyFormat = "$operand `,` $form attr-dict `:` functional-type(operands, results)";
}

def Normalize : NLNormalFormFunctionOp<"normalize">;
def IsNormalized : NLNormalFormFunctionOp<"is_normalized">;

def Substring : NLOp<"substring", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise substring [start, start + length)";

  let arguments = (ins NLChunk:$string, NLChunk:$start, Optional<NLChunk>:$length);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$string `,` $start (`,` $length^)? attr-dict `:` functional-type(operands, results)";
}

def Split : NLOp<"split", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise split on a delimiter, or on every delimiter of a list";

  let arguments = (ins NLChunk:$string, NLChunk:$delimiter);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$string `,` $delimiter attr-dict `:` functional-type(operands, results)";
}

class NLStringEndOp<string mnemonic> : NLOp<mnemonic, [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise run of characters at one end of a string";

  let arguments = (ins NLChunk:$string, NLChunk:$length);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$string `,` $length attr-dict `:` functional-type(operands, results)";
}

def Left : NLStringEndOp<"left">;
def Right : NLStringEndOp<"right">;

def Replace : NLOp<"replace", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise replacement of every occurrence of a search string";

  let arguments = (ins NLChunk:$string, NLChunk:$search, NLChunk:$replacement);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$string `,` $search `,` $replacement attr-dict `:` functional-type(operands, results)";
}

def PointDistance : NLOp<"point_distance", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise distance between two points, null across CRSs";

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def PointWithinBBox : NLOp<"point_within_bbox", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of a point against a box, null unless the three share a CRS";

  let arguments = (ins NLChunk:$point, NLChunk:$lowerLeft, NLChunk:$upperRight);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$point `,` $lowerLeft `,` $upperRight attr-dict `:` functional-type(operands, results)";
}

class NLBinaryFunctionOp<string mnemonic> : NLOp<mnemonic, [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Apply a binary scalar function row-wise to two value chunks";

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def CosineSimilarity : NLBinaryFunctionOp<"cosine_similarity">;
def EuclideanDistance : NLBinaryFunctionOp<"euclidean_distance">;

#endif // TURINGDB_NL_FUNCTION_OPS
