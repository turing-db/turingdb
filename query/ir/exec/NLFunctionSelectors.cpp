#include "NLFunctionSelectors.h"

#include "NLOps.h"
#include "StorageEnums.h"

#include "columns/Functions.h"

#include "NLExecutor.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

const std::unordered_map<std::string_view, NLUnaryFunctionSelector> unaryFunctionSelectors = {
    {"nl.labels",     &NLExecutor::selectLabels},
    {"nl.edge_type",  &NLExecutor::selectEdgeType},
    {"nl.keys",       &NLExecutor::selectKeys},
    {"nl.properties", &NLExecutor::selectProperties},
    {"nl.start_node", &NLExecutor::selectFunction<StartNodeFunction>},
    {"nl.end_node",   &NLExecutor::selectFunction<EndNodeFunction>},
    {"nl.to_integer", &NLExecutor::selectConversion<ToIntegerFunction>},
    {"nl.to_float",   &NLExecutor::selectConversion<ToFloatFunction>},
    {"nl.to_string",  &NLExecutor::selectConversion<ToStringFunction>},
    {"nl.to_boolean", &NLExecutor::selectConversion<ToBoolFunction>},
    {"nl.to_integer_or_null", &NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToIntegerFunction>, TaggedToIntegerOrNullFunction>},
    {"nl.to_float_or_null",   &NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToFloatFunction>, TaggedToFloatOrNullFunction>},
    {"nl.to_boolean_or_null", &NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToBoolFunction>, TaggedToBooleanOrNullFunction>},
    {"nl.to_string_or_null",  &NLExecutor::selectOrNull<&NLExecutor::selectConversion<ToStringFunction>, TaggedToStringOrNullFunction>},
    {"nl.to_integer_list",    &NLExecutor::selectFunction<ToIntegerListFunction>},
    {"nl.to_float_list",      &NLExecutor::selectFunction<ToFloatListFunction>},
    {"nl.to_boolean_list",    &NLExecutor::selectFunction<ToBooleanListFunction>},
    {"nl.to_string_list",     &NLExecutor::selectFunction<ToStringListFunction>},
    {"nl.to_datetime", &NLExecutor::selectConversion<ToDateTimeFunction>},
    {"nl.to_duration", &NLExecutor::selectConversion<ToDurationFunction>},
    {"nl.to_point", &NLExecutor::selectConversion<ToPointFunction>},

    {"nl.datetime_year",        &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Year>>},
    {"nl.datetime_month",       &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Month>>},
    {"nl.datetime_day",         &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Day>>},
    {"nl.datetime_hour",        &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Hour>>},
    {"nl.datetime_minute",      &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Minute>>},
    {"nl.datetime_second",      &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Second>>},
    {"nl.datetime_millisecond", &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Millisecond>>},
    {"nl.datetime_microsecond", &NLExecutor::selectFunction<DateTimeComponentFunction<DateTimePart::Microsecond>>},

    {"nl.duration_years",                  &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Years>>},
    {"nl.duration_quarters",               &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Quarters>>},
    {"nl.duration_months",                 &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Months>>},
    {"nl.duration_weeks",                  &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Weeks>>},
    {"nl.duration_days",                   &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Days>>},
    {"nl.duration_hours",                  &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Hours>>},
    {"nl.duration_minutes",                &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Minutes>>},
    {"nl.duration_seconds",                &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Seconds>>},
    {"nl.duration_milliseconds",           &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Milliseconds>>},
    {"nl.duration_microseconds",           &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::Microseconds>>},
    {"nl.duration_quarters_of_year",       &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::QuartersOfYear>>},
    {"nl.duration_months_of_year",         &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfYear>>},
    {"nl.duration_months_of_quarter",      &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MonthsOfQuarter>>},
    {"nl.duration_days_of_week",           &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::DaysOfWeek>>},
    {"nl.duration_minutes_of_hour",        &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MinutesOfHour>>},
    {"nl.duration_seconds_of_minute",      &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::SecondsOfMinute>>},
    {"nl.duration_milliseconds_of_second", &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MillisecondsOfSecond>>},
    {"nl.duration_microseconds_of_second", &NLExecutor::selectFunction<DurationComponentFunction<DurationPart::MicrosecondsOfSecond>>},

    {"nl.point_x",         &NLExecutor::selectFunction<PointComponentFunction<PointPart::X>>},
    {"nl.point_y",         &NLExecutor::selectFunction<PointComponentFunction<PointPart::Y>>},
    {"nl.point_z",         &NLExecutor::selectFunction<PointComponentFunction<PointPart::Z>>},
    {"nl.point_longitude", &NLExecutor::selectFunction<PointComponentFunction<PointPart::Longitude>>},
    {"nl.point_latitude",  &NLExecutor::selectFunction<PointComponentFunction<PointPart::Latitude>>},
    {"nl.point_height",    &NLExecutor::selectFunction<PointComponentFunction<PointPart::Height>>},
    {"nl.point_crs",       &NLExecutor::selectFunction<PointComponentFunction<PointPart::CRS>>},
    {"nl.point_srid",      &NLExecutor::selectFunction<PointComponentFunction<PointPart::SRID>>},

    {"nl.element_id", &NLExecutor::selectId},
    {"nl.size",       &NLExecutor::selectSize},
    {"nl.is_empty",   &NLExecutor::selectIsEmpty},
    {"nl.char_length", &NLExecutor::selectFunction<CharLengthFunction>},
    {"nl.head",       &NLExecutor::selectFunction<ListHeadFunction>},
    {"nl.last",       &NLExecutor::selectFunction<ListLastFunction>},
    {"nl.tail",       &NLExecutor::selectFunction<ListTailFunction>},

    {"nl.abs",      &NLExecutor::selectNumberFunction<AbsFunction>},
    {"nl.negate",   &NLExecutor::selectNegateFunction},
    {"nl.unary_plus", &NLExecutor::selectUnaryPlusFunction},
    {"nl.sign",     &NLExecutor::selectNumberFunction<SignFunction>},
    {"nl.ceil",     &NLExecutor::selectFloatFunction<FloatFunctionKind::Ceil>},
    {"nl.floor",    &NLExecutor::selectFloatFunction<FloatFunctionKind::Floor>},
    {"nl.round",    &NLExecutor::selectFloatFunction<FloatFunctionKind::Round>},
    {"nl.sqrt",     &NLExecutor::selectFloatFunction<FloatFunctionKind::Sqrt>},
    {"nl.exp",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Exp>},
    {"nl.log",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Log>},
    {"nl.log10",    &NLExecutor::selectFloatFunction<FloatFunctionKind::Log10>},
    {"nl.sin",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Sin>},
    {"nl.cos",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Cos>},
    {"nl.tan",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Tan>},
    {"nl.cot",      &NLExecutor::selectFloatFunction<FloatFunctionKind::Cot>},
    {"nl.asin",     &NLExecutor::selectFloatFunction<FloatFunctionKind::Asin>},
    {"nl.acos",     &NLExecutor::selectFloatFunction<FloatFunctionKind::Acos>},
    {"nl.atan",     &NLExecutor::selectFloatFunction<FloatFunctionKind::Atan>},
    {"nl.degrees",  &NLExecutor::selectFloatFunction<FloatFunctionKind::Degrees>},
    {"nl.radians",  &NLExecutor::selectFloatFunction<FloatFunctionKind::Radians>},
    {"nl.haversin", &NLExecutor::selectFloatFunction<FloatFunctionKind::Haversin>},

    {"nl.to_upper", &NLExecutor::selectFunction<ToUpperFunction>},
    {"nl.to_lower", &NLExecutor::selectFunction<ToLowerFunction>},
    {"nl.trim",     &NLExecutor::selectFunction<TrimFunction<TrimSide::Both>>},
    {"nl.ltrim",    &NLExecutor::selectFunction<TrimFunction<TrimSide::Start>>},
    {"nl.rtrim",    &NLExecutor::selectFunction<TrimFunction<TrimSide::End>>},
    {"nl.reverse",  &NLExecutor::selectReverse},
};

template <template <UnicodeNormalForm> typename NormalFormFunction>
NLUnaryFunctionSelector selectNormalFormFunction(storage::UnicodeNormalForm form) {
    switch (form) {
        case storage::UnicodeNormalForm::NFC:
            return &NLExecutor::selectFunction<NormalFormFunction<UnicodeNormalForm::NFC>>;
        break;
        case storage::UnicodeNormalForm::NFD:
            return &NLExecutor::selectFunction<NormalFormFunction<UnicodeNormalForm::NFD>>;
        break;
        case storage::UnicodeNormalForm::NFKC:
            return &NLExecutor::selectFunction<NormalFormFunction<UnicodeNormalForm::NFKC>>;
        break;
        case storage::UnicodeNormalForm::NFKD:
            return &NLExecutor::selectFunction<NormalFormFunction<UnicodeNormalForm::NFKD>>;
        break;
        default:
            bioassert(false, "Unhandled Unicode normal form");
            return nullptr;
        break;
    }
}

const std::unordered_map<std::string_view, NLBinaryFunctionSelector> binaryFunctionSelectors = {
    {"nl.cosine_similarity",  &NLExecutor::selectBinary<OP_FUNC_COSINE_SIMILARITY>},
    {"nl.euclidean_distance", &NLExecutor::selectBinary<OP_FUNC_EUCLIDEAN_DISTANCE>},
};

}

NLUnaryFunctionSelector db::lookupUnaryFunctionSelector(mlir::Operation& operation) {
    if (nl::Normalize normalize = mlir::dyn_cast<nl::Normalize>(operation)) {
        return selectNormalFormFunction<NormalizeFunction>(normalize.getForm());
    } else if (nl::IsNormalized isNormalized = mlir::dyn_cast<nl::IsNormalized>(operation)) {
        return selectNormalFormFunction<IsNormalizedFunction>(isNormalized.getForm());
    } else if (nl::EntityIDOf entityIDOf = mlir::dyn_cast<nl::EntityIDOf>(operation)) {
        const nl::ChunkType result = mlir::cast<nl::ChunkType>(entityIDOf.getResult().getType());
        const bool namesEdges = mlir::isa<storage::EdgeIDType>(result.getElementType());

        return namesEdges ? &NLExecutor::selectEntityIDOf<EdgeID> : &NLExecutor::selectEntityIDOf<NodeID>;
    }

    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = unaryFunctionSelectors.find(std::string_view(name.data(), name.size()));
    return it == unaryFunctionSelectors.end() ? nullptr : it->second;
}

NLBinaryFunctionSelector db::lookupBinaryFunctionSelector(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = binaryFunctionSelectors.find(std::string_view(name.data(), name.size()));
    return it == binaryFunctionSelectors.end() ? nullptr : it->second;
}
