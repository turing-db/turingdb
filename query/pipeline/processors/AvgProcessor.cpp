#include "AvgProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "metadata/PropertyType.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "TypeUtils.h"

#include "PipelineException.h"

using namespace db;

AvgProcessor::AvgProcessor()
{
}

AvgProcessor::~AvgProcessor() {
}

std::string AvgProcessor::describe() const {
    return fmt::format("AvgProcessor @={}", fmt::ptr(this));
}

AvgProcessor* AvgProcessor::create(PipelineV2* pipeline, ColumnTag colTag) {
    AvgProcessor* avg = new AvgProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, avg);
    avg->_input.setPort(input);
    avg->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, avg);
    avg->_output.setPort(output);
    avg->addOutput(output);

    avg->postCreate(pipeline);
    avg->_colTag = colTag;
    return avg;
}

void AvgProcessor::prepare(ExecutionContext* ctxt) {
    auto* avgColumn = dynamic_cast<ColumnConst<AvgType>*>(_output.getValue()->getColumn());
    if (!avgColumn) {
        throw PipelineException("AvgProcessor: avg column is not a ColumnConst<Double>");
    }

    _avgColumn = avgColumn;

    if (!_colTag.isValid()) {
        throw PipelineException("AvgProcessor: column tag must be valid (avg(*) is not supported)");
    }

    const NamedColumn* inputCol = _input.getDataframe()->getColumn(_colTag);
    if (!inputCol) [[unlikely]] {
        throw PipelineException("AvgProcessor: input column does not exist");
    }

    _col = inputCol->getColumn();

    markAsPrepared();
}

void AvgProcessor::reset() {
    markAsReset();
}

/// Functor to pass to the column dispatcher to accumulate the sum and count of non-null rows
class Averager {
public:
    explicit Averager(AvgProcessor::AvgType& sum, size_t& count)
        : _sum(sum),
          _count(count)
    {
    }

    ~Averager() = default;

    // e.g. AVG(n.score) where score is a nullable double: accumulate non-null values
    template <TypeConcepts::OptionalType T>
    void operator()(const ColumnVector<T>* typed) {
        // TODO @cyrus: Decide how to extract the numeric primitive from optional types.
        // For optional<Double>, item.value() gives the double. For optional<Integer>,
        // item.value() gives an int64 that we cast to double. The cast is implicit here
        // but it should be made explicit once the type trait for "numeric primitive" is
        // established (see also the accumulator type decision in AvgProcessor.h).
        for (const T& item : *typed) {
            if (item.has_value()) {
                _sum += static_cast<AvgProcessor::AvgType>(item.value());
                _count++;
            }
        }
    }

    // e.g. AVG(n.score) where score is a non-nullable numeric column: accumulate all values
    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        // TODO @cyrus: Decide which non-optional column types are valid inputs for avg().
        // Currently the function declarations only register Integer and Double overloads,
        // so the analyzer will reject non-numeric arguments before we reach here. But this
        // dispatch path is still reached for those types; confirm whether a runtime guard
        // is needed or whether the analyzer guarantee is sufficient.
        for (const T& item : *typed) {
            _sum += static_cast<AvgProcessor::AvgType>(item);
            _count++;
        }
    }

    // e.g. AVG(NULL): no values to accumulate
    constexpr void operator()(const ColumnConst<PropertyNull>*  /*unused*/) {
    }

private:
    AvgProcessor::AvgType& _sum;
    size_t& _count;
};

void AvgProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();
    inputPort->consume();

    const Dataframe* inputDf = _input.getDataframe();

    // _col is always valid for avg (avg(*) is rejected in prepare())
    AvgProcessor::AvgType localSum = 0.0;
    size_t localCount = 0;

    Averager averager(localSum, localCount);
    using Types = OutputtedTypes;
    using AvgDispatch =
        ColumnSingleDispatcher<Types::Allowed, Averager, Types::Excluded>;

    AvgDispatch::dispatch(_col, averager);

    _sumRunning += localSum;
    _countRunning += localCount;

    if (inputPort->isClosed()) {
        // TODO @cyrus: Decide the result when _countRunning == 0 (i.e. all inputs were null
        // or no rows were processed). OpenCypher specifies that avg() over an empty or
        // all-null set returns null. The current ColumnConst<Double> output type cannot
        // represent null. Options:
        //   (a) Change the output column to ColumnConst<std::optional<Double>>, which can
        //       encode null — but requires all downstream consumers to handle optional.
        //   (b) Return a sentinel value such as NaN or 0.0 and document it.
        //   (c) Add a separate null-flag column that the result serialiser checks.
        // Until this is resolved, the processor divides by zero and produces NaN when
        // _countRunning == 0, which is incorrect.
        const AvgType result = _countRunning > 0
            ? _sumRunning / static_cast<AvgType>(_countRunning)
            : 0.0; // TODO @cyrus: replace with proper null handling (see above)

        _avgColumn->set(result);
        _output.getPort()->writeData();
        finish();
    }
}
