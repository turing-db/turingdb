#include "AvgProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/Dataframe.h"
#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"

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
    bioassert(!avgColumn, "Invalid avg column.");

    _avgColumn = avgColumn;

    if (!_colTag.isValid()) {
        throw PipelineException("AvgProcessor: column tag must be valid.");
    }

    const NamedColumn* inputCol = _input.getDataframe()->getColumn(_colTag);
    if (!inputCol) {
        throw PipelineException("AvgProcessor: input column does not exist.");
    }

    _col = inputCol->getColumn();

    markAsPrepared();
}

void AvgProcessor::reset() {
    _sum = 0;
    _count = 0;
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
    template <typename T>
    void operator()(const ColumnOptVector<T>* typed) {
        for (const std::optional<T>& item : *typed) {
            if (item.has_value()) {
                _sum += static_cast<AvgProcessor::AvgType>(item.value());
                _count++;
            }
        }
    }

    // e.g. AVG(n.score) where score is a non-nullable numeric column: accumulate all values
    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        for (const T& item : *typed) {
            _sum += static_cast<AvgProcessor::AvgType>(item);
            _count++;
        }
    }

    constexpr void operator()(const ColumnConst<PropertyNull>*) {}

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        if (typed->empty()) {
            return;
        }

        const T& val = typed->getRaw();
        _sum += static_cast<AvgProcessor::AvgType>(val);
        _count++;
    }

    template <typename T>
    void operator()(const ColumnConst<std::optional<T>>* typed) {
        if (typed->empty()) {
            return;
        }

        const std::optional<T>& maybe = typed->getRaw();
        if (!maybe.has_value()) {
            return;
        }

        const T& val = maybe.value();
        _sum += static_cast<AvgProcessor::AvgType>(val);
        _count++;
    }

private:
    AvgProcessor::AvgType& _sum;
    size_t& _count;
};

void AvgProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();

    AvgProcessor::AvgType localSum = 0.0;
    size_t localCount = 0;

    Averager averager(localSum, localCount);
    using Types = NumericallyAggregatedTypes;
    using Dispatcher = ColumnSingleDispatcher<Types::Allowed, Averager, Types::Excluded>;

    Dispatcher::dispatch(_col, averager);

    _sum += localSum;
    _count += localCount;

    inputPort->consume();
    if (inputPort->isClosed()) {
        const AvgType result = _count == 0 ? 0 : _sum / _count;

        _avgColumn->set(result);
        _output.getPort()->writeData();
        finish();
    }
}
