#include "DBLoweringFunctionEmitters.h"

#include <string_view>
#include <unordered_map>

#include "DBOps.h"
#include "DBLoweringChunkTypes.h"

#include "DBColumnElement.h"

using namespace db;

namespace nl = mlir::nl;

namespace {

template <typename NLOp>
mlir::Value emitNLUnaryFunction(mlir::OpBuilder& builder,
                                mlir::Location loc,
                                nl::ChunkType resultType,
                                mlir::Value input,
                                mlir::Operation* source) {
    return builder.create<NLOp>(loc, resultType, input).getResult();
}

template <typename DBOp, typename NLOp>
mlir::Value emitNLNormalFormFunction(mlir::OpBuilder& builder,
                                     mlir::Location loc,
                                     nl::ChunkType resultType,
                                     mlir::Value input,
                                     mlir::Operation* source) {
    DBOp normalFormOp = mlir::cast<DBOp>(source);

    return builder.create<NLOp>(loc, resultType, input, normalFormOp.getForm()).getResult();
}

// A number or a duration is its own unary plus, which the analyzer already checked. Only a
// cell, whose type is the row's, is checked when the program runs.
mlir::Value emitUnaryPlus(mlir::OpBuilder& builder,
                          mlir::Location loc,
                          nl::ChunkType resultType,
                          mlir::Value input,
                          mlir::Operation* source) {
    if (!isTaggedCellChunk(input.getType())) {
        return input;
    }

    return builder.create<nl::UnaryPlus>(loc, resultType, input).getResult();
}

const std::unordered_map<std::string_view, UnaryFunctionLowering> unaryFunctionLowerings = {
    // Both read an entity column, which carries its null in the ID an OPTIONAL MATCH left
    // invalid rather than in an optional, so their result is nullable whatever the input
    // chunk's own type says
    {"db.labels",     {&emitNLUnaryFunction<nl::Labels>, ResultNullability::AlwaysNullable}},
    {"db.edge_type",  {&emitNLUnaryFunction<nl::EdgeType>, ResultNullability::AlwaysNullable}},
    {"db.keys",       {&emitNLUnaryFunction<nl::Keys>, ResultNullability::AlwaysNullable}},
    {"db.properties", {&emitNLUnaryFunction<nl::PropertyMap>, ResultNullability::AlwaysNullable}},

    // An end of an edge is a node, and a node column spells its null as an invalid ID, so
    // the result stays a plain node chunk where labels() and type() need a nullable one
    {"db.start_node", {&emitNLUnaryFunction<nl::StartNode>, ResultNullability::NeverNullable}},
    {"db.end_node",   {&emitNLUnaryFunction<nl::EndNode>, ResultNullability::NeverNullable}},

    {"db.to_integer", {&emitNLUnaryFunction<nl::ToInteger>, ResultNullability::AlwaysNullable}},
    {"db.to_float",   {&emitNLUnaryFunction<nl::ToFloat>, ResultNullability::AlwaysNullable}},
    {"db.to_boolean", {&emitNLUnaryFunction<nl::ToBoolean>, ResultNullability::AlwaysNullable}},
    {"db.to_datetime", {&emitNLUnaryFunction<nl::ToDateTime>, ResultNullability::AlwaysNullable}},
    {"db.to_duration", {&emitNLUnaryFunction<nl::ToDuration>, ResultNullability::AlwaysNullable}},
    {"db.to_point",    {&emitNLUnaryFunction<nl::ToPoint>, ResultNullability::AlwaysNullable}},

    // A calendar field of an instant is always readable, so the field is null exactly
    // where the instant it was read off is
    {"db.datetime_year",        {&emitNLUnaryFunction<nl::DateTimeYear>, ResultNullability::FollowsInput}},
    {"db.datetime_month",       {&emitNLUnaryFunction<nl::DateTimeMonth>, ResultNullability::FollowsInput}},
    {"db.datetime_day",         {&emitNLUnaryFunction<nl::DateTimeDay>, ResultNullability::FollowsInput}},
    {"db.datetime_hour",        {&emitNLUnaryFunction<nl::DateTimeHour>, ResultNullability::FollowsInput}},
    {"db.datetime_minute",      {&emitNLUnaryFunction<nl::DateTimeMinute>, ResultNullability::FollowsInput}},
    {"db.datetime_second",      {&emitNLUnaryFunction<nl::DateTimeSecond>, ResultNullability::FollowsInput}},
    {"db.datetime_millisecond", {&emitNLUnaryFunction<nl::DateTimeMillisecond>, ResultNullability::FollowsInput}},
    {"db.datetime_microsecond", {&emitNLUnaryFunction<nl::DateTimeMicrosecond>, ResultNullability::FollowsInput}},

    {"db.duration_years",                  {&emitNLUnaryFunction<nl::DurationYears>, ResultNullability::FollowsInput}},
    {"db.duration_quarters",               {&emitNLUnaryFunction<nl::DurationQuarters>, ResultNullability::FollowsInput}},
    {"db.duration_months",                 {&emitNLUnaryFunction<nl::DurationMonths>, ResultNullability::FollowsInput}},
    {"db.duration_weeks",                  {&emitNLUnaryFunction<nl::DurationWeeks>, ResultNullability::FollowsInput}},
    {"db.duration_days",                   {&emitNLUnaryFunction<nl::DurationDays>, ResultNullability::FollowsInput}},
    {"db.duration_hours",                  {&emitNLUnaryFunction<nl::DurationHours>, ResultNullability::FollowsInput}},
    {"db.duration_minutes",                {&emitNLUnaryFunction<nl::DurationMinutes>, ResultNullability::FollowsInput}},
    {"db.duration_seconds",                {&emitNLUnaryFunction<nl::DurationSeconds>, ResultNullability::FollowsInput}},
    {"db.duration_milliseconds",           {&emitNLUnaryFunction<nl::DurationMilliseconds>, ResultNullability::FollowsInput}},
    {"db.duration_microseconds",           {&emitNLUnaryFunction<nl::DurationMicroseconds>, ResultNullability::FollowsInput}},
    {"db.duration_quarters_of_year",       {&emitNLUnaryFunction<nl::DurationQuartersOfYear>, ResultNullability::FollowsInput}},
    {"db.duration_months_of_year",         {&emitNLUnaryFunction<nl::DurationMonthsOfYear>, ResultNullability::FollowsInput}},
    {"db.duration_months_of_quarter",      {&emitNLUnaryFunction<nl::DurationMonthsOfQuarter>, ResultNullability::FollowsInput}},
    {"db.duration_days_of_week",           {&emitNLUnaryFunction<nl::DurationDaysOfWeek>, ResultNullability::FollowsInput}},
    {"db.duration_minutes_of_hour",        {&emitNLUnaryFunction<nl::DurationMinutesOfHour>, ResultNullability::FollowsInput}},
    {"db.duration_seconds_of_minute",      {&emitNLUnaryFunction<nl::DurationSecondsOfMinute>, ResultNullability::FollowsInput}},
    {"db.duration_milliseconds_of_second", {&emitNLUnaryFunction<nl::DurationMillisecondsOfSecond>, ResultNullability::FollowsInput}},
    {"db.duration_microseconds_of_second", {&emitNLUnaryFunction<nl::DurationMicrosecondsOfSecond>, ResultNullability::FollowsInput}},

    {"db.point_x",         {&emitNLUnaryFunction<nl::PointX>, ResultNullability::FollowsInput}},
    {"db.point_y",         {&emitNLUnaryFunction<nl::PointY>, ResultNullability::FollowsInput}},
    {"db.point_z",         {&emitNLUnaryFunction<nl::PointZ>, ResultNullability::FollowsInput}},
    {"db.point_longitude", {&emitNLUnaryFunction<nl::PointLongitude>, ResultNullability::FollowsInput}},
    {"db.point_latitude",  {&emitNLUnaryFunction<nl::PointLatitude>, ResultNullability::FollowsInput}},
    {"db.point_height",    {&emitNLUnaryFunction<nl::PointHeight>, ResultNullability::FollowsInput}},
    {"db.point_crs",       {&emitNLUnaryFunction<nl::PointCRSName>, ResultNullability::FollowsInput}},
    {"db.point_srid",      {&emitNLUnaryFunction<nl::PointSRID>, ResultNullability::FollowsInput}},
    {"db.to_string",  {&emitNLUnaryFunction<nl::ToString>, ResultNullability::FollowsInput}},
    {"db.to_integer_or_null", {&emitNLUnaryFunction<nl::ToIntegerOrNull>, ResultNullability::AlwaysNullable}},
    {"db.to_float_or_null",   {&emitNLUnaryFunction<nl::ToFloatOrNull>, ResultNullability::AlwaysNullable}},
    {"db.to_boolean_or_null", {&emitNLUnaryFunction<nl::ToBooleanOrNull>, ResultNullability::AlwaysNullable}},
    {"db.to_string_or_null",  {&emitNLUnaryFunction<nl::ToStringOrNull>, ResultNullability::FollowsInput}},
    {"db.to_integer_list",    {&emitNLUnaryFunction<nl::ToIntegerList>, ResultNullability::FollowsInput}},
    {"db.to_float_list",      {&emitNLUnaryFunction<nl::ToFloatList>, ResultNullability::FollowsInput}},
    {"db.to_boolean_list",    {&emitNLUnaryFunction<nl::ToBooleanList>, ResultNullability::FollowsInput}},
    {"db.to_string_list",     {&emitNLUnaryFunction<nl::ToStringList>, ResultNullability::FollowsInput}},
    {"db.element_id", {&emitNLUnaryFunction<nl::ElementID>, ResultNullability::AlwaysNullable}},
    {"db.size",       {&emitNLUnaryFunction<nl::Size>, ResultNullability::FollowsInput}},
    {"db.is_empty",   {&emitNLUnaryFunction<nl::IsEmpty>, ResultNullability::FollowsInput}},
    {"db.char_length", {&emitNLUnaryFunction<nl::CharLength>, ResultNullability::FollowsInput}},
    {"db.head",       {&emitNLUnaryFunction<nl::Head>, ResultNullability::NeverNullable}},
    {"db.last",       {&emitNLUnaryFunction<nl::Last>, ResultNullability::NeverNullable}},
    {"db.tail",       {&emitNLUnaryFunction<nl::Tail>, ResultNullability::FollowsInput}},

    {"db.abs",      {&emitNLUnaryFunction<nl::Abs>, ResultNullability::FollowsInput}},
    {"db.negate",   {&emitNLUnaryFunction<nl::Negate>, ResultNullability::FollowsInput}},
    {"db.unary_plus", {&emitUnaryPlus, ResultNullability::FollowsInput}},
    {"db.sign",     {&emitNLUnaryFunction<nl::Sign>, ResultNullability::FollowsInput}},
    {"db.ceil",     {&emitNLUnaryFunction<nl::Ceil>, ResultNullability::FollowsInput}},
    {"db.floor",    {&emitNLUnaryFunction<nl::Floor>, ResultNullability::FollowsInput}},
    {"db.round",    {&emitNLUnaryFunction<nl::Round>, ResultNullability::FollowsInput}},
    {"db.sqrt",     {&emitNLUnaryFunction<nl::Sqrt>, ResultNullability::FollowsInput}},
    {"db.exp",      {&emitNLUnaryFunction<nl::Exp>, ResultNullability::FollowsInput}},
    {"db.log",      {&emitNLUnaryFunction<nl::Log>, ResultNullability::FollowsInput}},
    {"db.log10",    {&emitNLUnaryFunction<nl::Log10>, ResultNullability::FollowsInput}},
    {"db.sin",      {&emitNLUnaryFunction<nl::Sin>, ResultNullability::FollowsInput}},
    {"db.cos",      {&emitNLUnaryFunction<nl::Cos>, ResultNullability::FollowsInput}},
    {"db.tan",      {&emitNLUnaryFunction<nl::Tan>, ResultNullability::FollowsInput}},
    {"db.cot",      {&emitNLUnaryFunction<nl::Cot>, ResultNullability::FollowsInput}},
    {"db.asin",     {&emitNLUnaryFunction<nl::Asin>, ResultNullability::FollowsInput}},
    {"db.acos",     {&emitNLUnaryFunction<nl::Acos>, ResultNullability::FollowsInput}},
    {"db.atan",     {&emitNLUnaryFunction<nl::Atan>, ResultNullability::FollowsInput}},
    {"db.degrees",  {&emitNLUnaryFunction<nl::Degrees>, ResultNullability::FollowsInput}},
    {"db.radians",  {&emitNLUnaryFunction<nl::Radians>, ResultNullability::FollowsInput}},
    {"db.haversin", {&emitNLUnaryFunction<nl::Haversin>, ResultNullability::FollowsInput}},

    {"db.to_upper", {&emitNLUnaryFunction<nl::ToUpper>, ResultNullability::FollowsInput}},
    {"db.to_lower", {&emitNLUnaryFunction<nl::ToLower>, ResultNullability::FollowsInput}},
    {"db.trim",     {&emitNLUnaryFunction<nl::Trim>, ResultNullability::FollowsInput}},
    {"db.ltrim",    {&emitNLUnaryFunction<nl::LTrim>, ResultNullability::FollowsInput}},
    {"db.rtrim",    {&emitNLUnaryFunction<nl::RTrim>, ResultNullability::FollowsInput}},
    {"db.reverse",  {&emitNLUnaryFunction<nl::Reverse>, ResultNullability::FollowsInput}},
    {"db.normalize", {&emitNLNormalFormFunction<mlir::db::Normalize, nl::Normalize>, ResultNullability::FollowsInput}},
    {"db.is_normalized", {&emitNLNormalFormFunction<mlir::db::IsNormalized, nl::IsNormalized>, ResultNullability::FollowsInput}}
};

template <typename NLOp>
mlir::Value emitNLBinaryFunction(mlir::OpBuilder& builder,
                                 mlir::Location loc,
                                 nl::ChunkType resultType,
                                 mlir::Value lhs,
                                 mlir::Value rhs) {
    return builder.create<NLOp>(loc, resultType, lhs, rhs).getResult();
}

mlir::Type floatFunctionElement(mlir::OpBuilder& builder, mlir::Type lhsElement) {
    return builder.getF64Type();
}

const std::unordered_map<std::string_view, BinaryFunctionLowering> binaryFunctionLowerings = {
    {"db.cosine_similarity",  {&emitNLBinaryFunction<nl::CosineSimilarity>,  &floatFunctionElement}},
    {"db.euclidean_distance", {&emitNLBinaryFunction<nl::EuclideanDistance>, &floatFunctionElement}},
};

}

const UnaryFunctionLowering* db::lookupUnaryFunctionLowering(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = unaryFunctionLowerings.find(std::string_view(name.data(), name.size()));
    return it == unaryFunctionLowerings.end() ? nullptr : &it->second;
}

const BinaryFunctionLowering* db::lookupBinaryFunctionLowering(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = binaryFunctionLowerings.find(std::string_view(name.data(), name.size()));
    return it == binaryFunctionLowerings.end() ? nullptr : &it->second;
}
