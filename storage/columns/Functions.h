#pragma once

#include <math.h>

#include <limits>
#include <numbers>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

#include "columns/ColumnConst.h"
#include "columns/ValueText.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "TypeUtils.h"
#include "list/ListElementView.h"

#include "list/ListBuffer.h"
#include "map/MapView.h"

#include "buffers/StringBuffer.h"

#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"

#include "views/GraphView.h"

#include "ID.h"

#include "TypeUtils.h"

#include "BioAssert.h"
#include "FatalException.h"

namespace db {

class CommitWriteBuffer;

/**
 * @brief Generic function to apply a generic invokable to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the invokable otherwise.
 */
template <typename Func, typename T, typename U>
    requires OptionallyInvokable<Func, T, U>
auto optionalGenericFunc(T&& a, U&& b) -> TypeUtils::optional_invoke_result<Func, T, U> {
    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (TypeUtils::is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = TypeUtils::unwrap(a);
    auto&& bv = TypeUtils::unwrap(b);

    return Func {}(av, bv);
}

class LabelsFunction {
public:
    using ArgType = NodeID;
    using ResultType = ListView;

    LabelsFunction(GraphView view, QueryListBuffer* listBuffer);

    // The graph holds none of a change's writes until they commit, so the labels of a
    // node this one wrote are read out of @param writeBuffer
    LabelsFunction(GraphView view,
                   QueryListBuffer* listBuffer,
                   const CommitWriteBuffer* writeBuffer);

    ResultType operator()(NodeID node);

private:
    GraphView _view;
    QueryListBuffer* _listBuffer {nullptr};
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingNodeID {0};
    std::vector<LabelID> _labels;
    std::vector<QueryListBuffer::ListItemVariant> _elements;

    bool isPendingNode(NodeID node) const;
    LabelSetHandle readLabelSet(NodeID node) const;
};

class EdgeTypesFunction {
public:
    using ArgType = EdgeID;
    using ResultType = std::string_view;

    EdgeTypesFunction(GraphView view, StringBuffer* stringBuffer);

    // The graph holds none of a change's writes until they commit, so the type of an edge
    // this one wrote is read out of @param writeBuffer
    EdgeTypesFunction(GraphView view,
                      StringBuffer* stringBuffer,
                      const CommitWriteBuffer* writeBuffer);

    ResultType operator()(EdgeID edge);

private:
    GraphView _view;
    StringBuffer* _stringBuffer {nullptr};
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingEdgeID {0};

    EdgeTypeID readEdgeType(EdgeID edge) const;
};

// The ends of an edge, read as the graph stores it: the start is the node the edge leaves
// and the end the node it reaches, whichever way the pattern walked it. An invalid edge -
// what an OPTIONAL MATCH leaves where it matched nothing - has no end, so the answer is the
// invalid node ID a node column carries its null as.
class EdgeEndsFunction {
public:
    using ArgType = EdgeID;
    using ResultType = NodeID;

    explicit EdgeEndsFunction(GraphView view);

    // The graph holds none of a change's writes until they commit, so the ends of an edge
    // this one wrote are read out of @param writeBuffer
    EdgeEndsFunction(GraphView view, const CommitWriteBuffer* writeBuffer);

protected:
    NodeID getStartNode(EdgeID edge) const;
    NodeID getEndNode(EdgeID edge) const;

private:
    GraphView _view;
    const CommitWriteBuffer* _writeBuffer {nullptr};
    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    void readEnds(EdgeID edge, NodeID& start, NodeID& end) const;
    void readPendingEnds(EdgeID edge, NodeID& start, NodeID& end) const;
};

// The same two over a type-erased cell, which is what an UNWIND or an index of a stored
// list binds: a stored list names no element type, so what its elements are is known per
// row rather than in the plan. A cell holding a null answers the invalid node such a
// column carries; one holding no edge at all is the type error the row raises.
class TaggedStartNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using ArgType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedEndNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using ArgType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class StartNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using TaggedCounterpart = TaggedStartNodeFunction;

    ResultType operator()(const EdgeID edge) const { return getStartNode(edge); }
};

class EndNodeFunction : public EdgeEndsFunction {
public:
    using EdgeEndsFunction::EdgeEndsFunction;
    using TaggedCounterpart = TaggedEndNodeFunction;

    ResultType operator()(const EdgeID edge) const { return getEndNode(edge); }
};

class TaggedIdFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(ArgType cell) const;
};

template <TypedInternalID IDT>
class IdFunction {
public:
    using ArgType = IDT;
    using ResultType = types::Int64::Primitive;

    ResultType operator()(const IDT id) const { return static_cast<types::Int64::Primitive>(id.getValue()); }
};

class ToIntegerFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(std::string_view sv) {
        types::Int64::Primitive result = 0;

        const auto* start = sv.data();
        const auto* end = sv.data() + sv.size();

        auto [ptr, ec] = std::from_chars(start, end, result);

        const bool success = ec == std::errc {};
        const bool parsedAll = ptr == end;

        if (!success || !parsedAll) {
            return std::nullopt;
        }

        return result;
    }
};

// NOTE: macOS wheel build libc++ doesn't support from_chars on double: use strtod instead
class ToFloatFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Double::Primitive>;

    ResultType operator()(std::string_view sv) {
        // @ref std::strtod requires a null-terminated std::string: convert the string_view
        _buf.assign(sv);
        char* end = nullptr;
        errno = 0;

        const types::Double::Primitive result = std::strtod(_buf.data(), &end);

        // Either empty string, or could not parse all of the string as double
        const bool couldNotConvert = end == _buf.data() || end != _buf.data() + _buf.size();
        if (couldNotConvert) {
            return std::nullopt;
        }

        const bool outOfRange = errno == ERANGE;
        if (outOfRange) {
            return std::nullopt;
        }

        return result;
    }

private:
    std::string _buf; // Temporary buffer to null-terminate string view
};

class ToBoolFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::Bool::Primitive>;

    ResultType operator()(std::string_view sv) {
        strToLower(_buf, sv);

        if (_buf == "true") {
            return true;
        }
        if (_buf == "false") {
            return false;
        }

        return std::nullopt;
    }

private:
    std::string _buf; // Preallocated buffer to reuse over iterations

    static void strToLower(std::string& lower, std::string_view src);
};

class ToDateTimeFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = std::optional<types::DateTime::Primitive>;

    ResultType operator()(std::string_view sv) {
        return DateTime::parse(sv);
    }
};

class ToStringFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::String::Primitive;

    explicit ToStringFunction(StringBuffer* stringBuffer);

    ResultType operator()(std::string_view sv) const {
        return _stringBuffer->insert(sv);
    }

private:
    StringBuffer* _stringBuffer {nullptr};
};

// toString() of a number or a boolean. A double keeps the point Cypher prints it with, so
// toString(1.0) is "1.0" where the shortest round trip of it is "1".
template <typename Value>
class ToStringFromValueFunction {
public:
    using ArgType = Value;
    using ResultType = types::String::Primitive;

    explicit ToStringFromValueFunction(StringBuffer* stringBuffer)
        : _stringBuffer(stringBuffer)
    {
    }

    ResultType operator()(const Value value) const {
        ValueTextScratch scratch;

        return _stringBuffer->insert(valueTextInto(value, scratch));
    }

private:
    StringBuffer* _stringBuffer {nullptr};
};

// datetime() over a count of seconds since the Unix epoch rather than over text. A count
// naming an instant outside the renderable range reads as null, as unparsable text does.
template <typename Number>
class EpochSecondsToDateTimeFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::DateTime::Primitive>;

    ResultType operator()(const Number seconds) {
        if constexpr (std::is_unsigned_v<Number>) {
            if (seconds > static_cast<Number>(std::numeric_limits<types::Int64::Primitive>::max())) {
                return std::nullopt;
            }
        }

        types::DateTime::Primitive value;
        if (!DateTime::fromEpochSeconds(static_cast<types::Int64::Primitive>(seconds), value)) {
            return std::nullopt;
        }

        return value;
    }
};

// duration() over a count of microseconds. A UInt64 count past INT64_MAX has no duration
// and reads as null.
template <typename Number>
class MicrosecondsToDurationFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::Duration::Primitive>;

    ResultType operator()(const Number microseconds) {
        if constexpr (std::is_unsigned_v<Number>) {
            if (microseconds > static_cast<Number>(std::numeric_limits<types::Int64::Primitive>::max())) {
                return std::nullopt;
            }
        }

        return types::Duration::Primitive {static_cast<types::Int64::Primitive>(microseconds)};
    }
};

class MapToDurationFunction {
public:
    using ArgType = MapView;
    using ResultType = std::optional<types::Duration::Primitive>;

    ResultType operator()(const MapView map) const {
        types::Duration::Primitive value;
        if (!Duration::fromMap(map, value)) {
            return std::nullopt;
        }

        return value;
    }
};

class TaggedDurationFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Duration::Primitive>;

    ResultType operator()(const ArgType cell) const {
        const ListBufferTypeTag tag = cell.getTag();

        if (tag == ListBufferTypeTag::Int) {
            return MicrosecondsToDurationFunction<types::Int64::Primitive> {}(cell.getAs<types::Int64::Primitive>());
        } else if (tag == ListBufferTypeTag::UInt) {
            return MicrosecondsToDurationFunction<types::UInt64::Primitive> {}(cell.getAs<types::UInt64::Primitive>());
        } else if (tag == ListBufferTypeTag::MapView) {
            return MapToDurationFunction {}(cell.getAs<MapView>());
        } else if (tag == ListBufferTypeTag::Null) {
            return std::nullopt;
        }

        throw FatalException("duration() reads an integer or a map, and this row holds a value that is neither");
    }

    ResultType operator()(const std::optional<ArgType>& cell) const {
        return cell.has_value() ? (*this)(*cell) : std::nullopt;
    }
};

// The field of an instant a type-erased cell carries, which is what an UNWIND of a list of
// instants hands each row. A cell holding a null answers null, as every function over a
// cell does; one holding no instant is a type error only the row it is in can find out
// about, so it throws.
template <DateTimePart part>
class TaggedDateTimeComponentFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(const ArgType cell) const {
        const ListBufferTypeTag tag = cell.getTag();

        if (tag == ListBufferTypeTag::DateTime) {
            return DateTime::component(cell.getAs<types::DateTime::Primitive>(), part);
        } else if (tag == ListBufferTypeTag::Null) {
            return std::nullopt;
        }

        throw FatalException("a datetime component reads an instant, and this row holds a value that is not one");
    }

    ResultType operator()(const std::optional<ArgType>& cell) const {
        return cell.has_value() ? (*this)(*cell) : std::nullopt;
    }
};

template <DateTimePart part>
class DateTimeComponentFunction {
public:
    using ArgType = types::DateTime::Primitive;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedDateTimeComponentFunction<part>;

    ResultType operator()(const types::DateTime::Primitive value) {
        return DateTime::component(value, part);
    }
};

template <DurationPart part>
class TaggedDurationComponentFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(const ArgType cell) const {
        const ListBufferTypeTag tag = cell.getTag();

        if (tag == ListBufferTypeTag::Duration) {
            return Duration::component(cell.getAs<types::Duration::Primitive>(), part);
        } else if (tag == ListBufferTypeTag::Null) {
            return std::nullopt;
        }

        // We throw here as we only know the type of the list element at runtime.
        throw FatalException("a duration component reads a duration, and this row holds a value that is not one");
    }

    ResultType operator()(const std::optional<ArgType>& cell) const {
        return cell.has_value() ? (*this)(*cell) : std::nullopt;
    }
};

template <DurationPart part>
class DurationComponentFunction {
public:
    using ArgType = types::Duration::Primitive;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedDurationComponentFunction<part>;

    ResultType operator()(const types::Duration::Primitive value) {
        return Duration::component(value, part);
    }
};

// toInteger() and toFloat() over a number rather than a string. Every conversion keeps an
// optional result so one column type carries it whatever the argument was, and so a double
// that no integer can represent reads as null.
template <typename Number>
class ToIntegerFromNumberFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(const Number value) {
        if constexpr (std::is_floating_point_v<Number>) {
            const types::Double::Primitive truncated = trunc(static_cast<types::Double::Primitive>(value));

            // The smallest int64 converts to a double exactly; the largest does not, so the
            // upper bound is the power of two just past it and the comparison excludes it
            const types::Double::Primitive lowest
                = static_cast<types::Double::Primitive>(std::numeric_limits<types::Int64::Primitive>::min());
            const types::Double::Primitive pastHighest = -lowest;

            const bool representable = truncated >= lowest && truncated < pastHighest;
            if (!representable) {
                return std::nullopt;
            }

            return static_cast<types::Int64::Primitive>(truncated);
        } else {
            return static_cast<types::Int64::Primitive>(value);
        }
    }
};

template <typename Number>
class ToFloatFromNumberFunction {
public:
    using ArgType = Number;
    using ResultType = std::optional<types::Double::Primitive>;

    ResultType operator()(const Number value) {
        return static_cast<types::Double::Primitive>(value);
    }
};

template <typename Argument>
concept ConvertibleNumber = std::is_arithmetic_v<Argument>
                            && !std::is_same_v<Argument, types::Bool::Primitive>;

// The element a column of the given type converts, with any optional unwrapped: what a
// conversion is applied to row by row.
template <typename ColumnType>
using ConversionArgument = TypeUtils::unwrap_inner_t<TypeUtils::decay_col_t<ColumnType>>;

// The conversion a column of the given element type goes through. A conversion names its
// string form, which stays the one every non-numeric element converts through.
template <typename StringFunctor, typename Argument>
struct ConversionFunctorFor {
    using Type = StringFunctor;
};

template <ConvertibleNumber Argument>
struct ConversionFunctorFor<ToIntegerFunction, Argument> {
    using Type = ToIntegerFromNumberFunction<Argument>;
};

template <ConvertibleNumber Argument>
struct ConversionFunctorFor<ToFloatFunction, Argument> {
    using Type = ToFloatFromNumberFunction<Argument>;
};

template <ConvertibleNumber Argument>
struct ConversionFunctorFor<ToStringFunction, Argument> {
    using Type = ToStringFromValueFunction<Argument>;
};

// A function over a type-erased cell finds out per row what the cell holds. A null answers
// null; any other type the function does not read is the row's type error.
[[noreturn]] void throwCellTypeError(std::string_view functionName, std::string_view expected);

template <typename Result, typename Visit>
Result visitNumberCell(const ListElementView cell, std::string_view functionName, Visit&& visit) {
    const ListBufferTypeTag tag = cell.getTag();

    if (tag == ListBufferTypeTag::Int) {
        return visit(cell.getAs<types::Int64::Primitive>());
    } else if (tag == ListBufferTypeTag::UInt) {
        return visit(cell.getAs<types::UInt64::Primitive>());
    } else if (tag == ListBufferTypeTag::Double) {
        return visit(cell.getAs<types::Double::Primitive>());
    } else if (tag == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    throwCellTypeError(functionName, "a number");
}

std::optional<types::String::Primitive> cellString(const ListElementView cell, std::string_view functionName);

std::optional<types::Int64::Primitive> cellInteger(const ListElementView cell, std::string_view functionName);

// abs keeps the type each cell holds, so its answer is a cell of its own
class TaggedAbsFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<ListElementView>;

    explicit TaggedAbsFunction(QueryListBuffer* listBuffer);

    ResultType operator()(const ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;

private:
    QueryListBuffer* _listBuffer {nullptr};
};

template <typename Number>
class AbsFunction {
public:
    using ArgType = Number;
    using ResultType = Number;
    using TaggedCounterpart = TaggedAbsFunction;

    ResultType operator()(const Number value) const {
        if constexpr (std::is_unsigned_v<Number>) {
            return value;
        } else if constexpr (std::is_floating_point_v<Number>) {
            return fabs(value);
        } else {
            // -value is undefined on the lowest int64; negating its unsigned form wraps it
            // onto itself, as Java's Math.abs does
            using Unsigned = std::make_unsigned_t<Number>;
            return value < 0 ? static_cast<Number>(Unsigned {0} - static_cast<Unsigned>(value)) : value;
        }
    }
};

class TaggedSignFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(const ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

template <typename Number>
class SignFunction {
public:
    using ArgType = Number;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedSignFunction;

    ResultType operator()(const Number value) const {
        return static_cast<ResultType>(value > Number {0}) - static_cast<ResultType>(value < Number {0});
    }
};

enum class FloatFunctionKind : uint8_t {
    Ceil,
    Floor,
    Round,
    Sqrt,
    Exp,
    Log,
    Log10,
    Sin,
    Cos,
    Tan,
    Cot,
    Asin,
    Acos,
    Atan,
    Degrees,
    Radians,
    Haversin,
};

std::string_view floatFunctionName(FloatFunctionKind kind);

template <FloatFunctionKind Kind>
class TaggedFloatFunction;

template <FloatFunctionKind Kind, typename Number>
class FloatFunction {
public:
    using ArgType = Number;
    using ResultType = types::Double::Primitive;
    using TaggedCounterpart = TaggedFloatFunction<Kind>;

    ResultType operator()(const Number number) const {
        const types::Double::Primitive value = static_cast<types::Double::Primitive>(number);

        if constexpr (Kind == FloatFunctionKind::Ceil) {
            return ceil(value);
        } else if constexpr (Kind == FloatFunctionKind::Floor) {
            return floor(value);
        } else if constexpr (Kind == FloatFunctionKind::Round) {
            // Cypher rounds a half-way value up, so round(-2.5) is -2 where C's round gives -3
            const types::Double::Primitive floored = floor(value);
            return value - floored >= 0.5 ? floored + 1.0 : floored;
        } else if constexpr (Kind == FloatFunctionKind::Sqrt) {
            return sqrt(value);
        } else if constexpr (Kind == FloatFunctionKind::Exp) {
            return exp(value);
        } else if constexpr (Kind == FloatFunctionKind::Log) {
            return log(value);
        } else if constexpr (Kind == FloatFunctionKind::Log10) {
            return log10(value);
        } else if constexpr (Kind == FloatFunctionKind::Sin) {
            return sin(value);
        } else if constexpr (Kind == FloatFunctionKind::Cos) {
            return cos(value);
        } else if constexpr (Kind == FloatFunctionKind::Tan) {
            return tan(value);
        } else if constexpr (Kind == FloatFunctionKind::Cot) {
            return 1.0 / tan(value);
        } else if constexpr (Kind == FloatFunctionKind::Asin) {
            return asin(value);
        } else if constexpr (Kind == FloatFunctionKind::Acos) {
            return acos(value);
        } else if constexpr (Kind == FloatFunctionKind::Atan) {
            return atan(value);
        } else if constexpr (Kind == FloatFunctionKind::Degrees) {
            return value * (180.0 / std::numbers::pi);
        } else if constexpr (Kind == FloatFunctionKind::Radians) {
            return value * (std::numbers::pi / 180.0);
        } else {
            static_assert(Kind == FloatFunctionKind::Haversin);
            return (1.0 - cos(value)) / 2.0;
        }
    }
};

template <FloatFunctionKind Kind>
class TaggedFloatFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Double::Primitive>;

    ResultType operator()(const ArgType cell) const {
        return visitNumberCell<ResultType>(cell, floatFunctionName(Kind), []<typename Number>(const Number number) -> ResultType {
            return FloatFunction<Kind, Number> {}(number);
        });
    }

    ResultType operator()(const std::optional<ArgType>& cell) const {
        return cell.has_value() ? (*this)(*cell) : std::nullopt;
    }
};

template <typename TextFunctor>
class TaggedTextFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::String::Primitive>;

    TaggedTextFunction()
        requires std::is_default_constructible_v<TextFunctor>
    {
    }

    explicit TaggedTextFunction(StringBuffer* stringBuffer)
        requires std::is_constructible_v<TextFunctor, StringBuffer*>
        : _functor(stringBuffer)
    {
    }

    ResultType operator()(const ArgType cell) const {
        const std::optional<types::String::Primitive> text = cellString(cell, TextFunctor::NAME);

        return text.has_value() ? ResultType {_functor(*text)} : std::nullopt;
    }

    ResultType operator()(const std::optional<ArgType>& cell) const {
        return cell.has_value() ? (*this)(*cell) : std::nullopt;
    }

private:
    TextFunctor _functor;
};

// Only ASCII letters change case: every byte of a multi-byte UTF-8 character is at least
// 0x80, so 'é' passes through both unchanged.
class ToUpperFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::String::Primitive;
    using TaggedCounterpart = TaggedTextFunction<ToUpperFunction>;

    static constexpr std::string_view NAME = "toUpper";

    explicit ToUpperFunction(StringBuffer* stringBuffer);

    ResultType operator()(const ArgType string) const;

private:
    StringBuffer* _stringBuffer {nullptr};
};

class ToLowerFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::String::Primitive;
    using TaggedCounterpart = TaggedTextFunction<ToLowerFunction>;

    static constexpr std::string_view NAME = "toLower";

    explicit ToLowerFunction(StringBuffer* stringBuffer);

    ResultType operator()(const ArgType string) const;

private:
    StringBuffer* _stringBuffer {nullptr};
};

enum class TrimSide : uint8_t {
    Start,
    End,
    Both,
};

template <TrimSide Side>
class TrimFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::String::Primitive;
    using TaggedCounterpart = TaggedTextFunction<TrimFunction<Side>>;

    static constexpr std::string_view NAME = Side == TrimSide::Start ? "ltrim"
                                           : Side == TrimSide::End   ? "rtrim"
                                                                     : "trim";

    ResultType operator()(const ArgType string) const {
        constexpr std::string_view whitespace = " \t\n\v\f\r";

        ResultType trimmed = string;

        if constexpr (Side != TrimSide::End) {
            const size_t first = trimmed.find_first_not_of(whitespace);
            trimmed.remove_prefix(first == std::string_view::npos ? trimmed.size() : first);
        }

        if constexpr (Side != TrimSide::Start) {
            const size_t last = trimmed.find_last_not_of(whitespace);
            trimmed.remove_suffix(last == std::string_view::npos ? trimmed.size() : trimmed.size() - last - 1);
        }

        return trimmed;
    }
};

class SubstringFunction {
public:
    using ResultType = types::String::Primitive;

    static constexpr std::string_view NAME = "substring";

    static constexpr types::Int64::Primitive MAX_ARGUMENT = std::numeric_limits<int32_t>::max();

    ResultType operator()(types::String::Primitive string,
                          types::Int64::Primitive start,
                          types::Int64::Primitive length) const;
};

class ListReverseFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = types::List::Primitive;

    explicit ListReverseFunction(QueryListBuffer* listBuffer);

    ResultType operator()(const ArgType list) const;

private:
    QueryListBuffer* _listBuffer {nullptr};
};

class ReverseFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::String::Primitive;

    explicit ReverseFunction(StringBuffer* stringBuffer);

    ResultType operator()(const ArgType string) const;

private:
    StringBuffer* _stringBuffer {nullptr};
};

// A cell may hold a string or a list, so its reversal is a cell of its own
class TaggedReverseFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<ListElementView>;

    TaggedReverseFunction(QueryListBuffer* listBuffer, StringBuffer* stringBuffer);

    ResultType operator()(const ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;

private:
    QueryListBuffer* _listBuffer {nullptr};
    StringBuffer* _stringBuffer {nullptr};
};

// The list family over a type-erased cell, which is what a list looks like wherever its
// type is known only per row - an element an UNWIND of a list of lists hands on. A cell
// holding a null answers null, as Cypher answers a list function over one; one holding
// no list - and, under size(), no string - is a type error only the row it is in can find
// out about, so each throws.

class TaggedSizeFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::Int64::Primitive>;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListHeadFunction {
public:
    using ArgType = ListElementView;
    using ResultType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListLastFunction {
public:
    using ArgType = ListElementView;
    using ResultType = ListElementView;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class TaggedListTailFunction {
public:
    using ArgType = ListElementView;
    using ResultType = std::optional<types::List::Primitive>;

    ResultType operator()(ArgType cell) const;
    ResultType operator()(const std::optional<ArgType>& cell) const;
};

class ListSizeFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedSizeFunction;

    ResultType operator()(const ArgType list) const {
        return static_cast<ResultType>(list.size());
    }
};

// The size of a string is the number of Unicode characters it holds, not the number of
// bytes its UTF-8 encoding spends on them.
class StringSizeFunction {
public:
    using ArgType = types::String::Primitive;
    using ResultType = types::Int64::Primitive;
    using TaggedCounterpart = TaggedSizeFunction;

    ResultType operator()(const ArgType string) const;
};

class ListHeadFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = ListElementView;
    using TaggedCounterpart = TaggedListHeadFunction;

    // An empty list has no first element and an absent one has no element at all, so both
    // head into the null a tagged cell carries itself: the result needs no nullable column
    static constexpr bool ReadsNullsItself = true;

    ResultType operator()(const ArgType list) const {
        return list.empty() ? ListElementView::nullElement() : list.front();
    }

    ResultType operator()(const std::optional<ArgType>& list) const {
        return list.has_value() ? (*this)(*list) : ListElementView::nullElement();
    }
};

class ListLastFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = ListElementView;
    using TaggedCounterpart = TaggedListLastFunction;

    static constexpr bool ReadsNullsItself = true;

    ResultType operator()(const ArgType list) const {
        return list.empty() ? ListElementView::nullElement() : list.back();
    }

    ResultType operator()(const std::optional<ArgType>& list) const {
        return list.has_value() ? (*this)(*list) : ListElementView::nullElement();
    }
};

class ListTailFunction {
public:
    using ArgType = types::List::Primitive;
    using ResultType = types::List::Primitive;
    using TaggedCounterpart = TaggedListTailFunction;

    ResultType operator()(const ArgType list) const {
        return list.tail();
    }
};

// A function that reads its own nulls is handed the absent value itself and answers with a
// value of its result type, so its result rides a plain column rather than a nullable one.
template <typename Functor>
concept ReadsItsNulls = requires { requires Functor::ReadsNullsItself; };

// A function naming a counterpart reads the same argument out of a type-erased cell, so a
// column of cells is served by that one rather than by the function itself.
template <typename Functor>
concept HasTaggedCounterpart = requires { typename Functor::TaggedCounterpart; };

class CosineSimilarityFunction {
public:
    using ResultType = types::Double::Primitive;

    ResultType operator()(const types::Embedding::Primitive& a,
                          const types::Embedding::Primitive& b);
};

class EuclideanDistanceFunction {
public:
    using ResultType = types::Double::Primitive;

    ResultType operator()(const types::Embedding::Primitive& a,
                          const types::Embedding::Primitive& b);
};

/// Generic function executor; default constructible operator
template <typename Op, typename Res, typename Arg>
struct FunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<Arg>* arg) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resd[i] = op(argd[i]);
        }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<Arg>* arg) {
        const auto& argd = arg->getRaw();

        auto op = Op {};
        auto&& result = op(argd);
        res->set(result);
    }
};

/// Specialisation for labels()
template <typename Res, typename Arg>
struct FunctionExecutor<LabelsFunction, Res, Arg> {
    static void apply(ColumnVector<ListView>* res,
                      const ColumnNodeIDs* arg,
                      GraphView view,
                      QueryListBuffer* listBuffer) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        LabelsFunction labels(view, listBuffer);
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = labels(argd[i]);
        }
    }
};

/// Specialisation for type()
template <typename Res, typename Arg>
struct FunctionExecutor<EdgeTypesFunction, Res, Arg> {
    static void apply(ColumnVector<std::string_view>* res,
                      const ColumnEdgeIDs* arg,
                      GraphView view,
                      StringBuffer* stringBuffer) {
        const size_t size = arg->size();
        res->resize(size);

        const auto& argd = arg->getRaw();
        auto& resd = res->getRaw();

        EdgeTypesFunction edgeType(view, stringBuffer);
        for (size_t i = 0; i < size ; i ++) {
            resd[i] = edgeType(argd[i]);
        }
    }
};

template <typename Op, typename Res, typename ArgA, typename ArgB>
struct BinaryFunctionExecutor {
    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();

        res->resize(size);

        const auto& lhsRaw = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(lhsRaw[i], rhsRaw[i]);
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsRaw = lhs->getRaw();
        const auto& val = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(lhsRaw[i]), TypeUtils::unwrap(val));
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnVector<ArgB>* rhs) {
        const size_t size = rhs->size();
        res->resize(size);


        const auto& val = lhs->getRaw();
        const auto& rhsRaw = rhs->getRaw();
        auto& resRaw = res->getRaw();

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            resRaw[i] = op(TypeUtils::unwrap(val), TypeUtils::unwrap(rhsRaw[i]));
        }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<ArgA>* lhs,
                      const ColumnConst<ArgB>* rhs) {
        auto op = Op {};
        const Res& result = op(lhs->getRaw(), rhs->getRaw());
        res->set(result);
    }
};

template <typename F>
struct BinaryFunc {
    template <typename T, typename U>
        requires TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return optionalGeneric<F>(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }
};

using CosineSimilarity = BinaryFunc<CosineSimilarityFunction>;
using EuclideanDistance = BinaryFunc<EuclideanDistanceFunction>;

}
