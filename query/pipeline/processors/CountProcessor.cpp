#include "CountProcessor.h"

#include <limits>
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

CountProcessor::CountProcessor()
{
}

CountProcessor::~CountProcessor() {
}

std::string CountProcessor::describe() const {
    return fmt::format("CountProcessor @={}", fmt::ptr(this));
}

CountProcessor* CountProcessor::create(PipelineV2* pipeline, ColumnTag colTag) {
    CountProcessor* count = new CountProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, count);
    count->_input.setPort(input);
    count->addInput(input);
    
    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, count);
    count->_output.setPort(output);
    count->addOutput(output);

    count->postCreate(pipeline);
    count->_colTag = colTag;
    return count;
}

void CountProcessor::prepare(ExecutionContext* /*ctxt*/) {
    auto* countColumn = dynamic_cast<ColumnConst<types::UInt64::Primitive>*>(_output.getValue()->getColumn());
    if (!countColumn) {
        throw PipelineException("CountProcessor: count column is not a ColumnConst<UInt64>");
    }

    _countColumn = countColumn;

    if (!_colTag.isValid()) {
        // If column tag is not set, we are just counting the number of rows in the block
        // In cypher this is done with count(*)
        return;
    }

    // Otherwise, we are counting the number of non-null values in the column
    // e.g. in cypher count(n.name)
    const NamedColumn* inputCol = _input.getDataframe()->getColumn(_colTag);
    if (!inputCol) [[unlikely]] {
        throw PipelineException("CountProcessor: input column does not exist");
    }

    _col = inputCol->getColumn();

    markAsPrepared();
}

void CountProcessor::reset() {
    markAsReset();
}

template <typename T, template<typename...> class Template>
struct is_template : std::false_type {};

template <template<typename...> class Template, typename... Args>
struct is_template<Template<Args...>, Template> : std::true_type {};

template <typename T, template<typename...> class Template>
inline constexpr bool is_template_t = is_template<T, Template>::value;

/// Functor to pass to column dispatcher to count rows
struct Counter {
    CountProcessor::CountType& _count;

    // e.g. COUNT(n.name) : count only non-null valued rows
    template <TypeConcepts::OptionalType T>
    void operator()(const ColumnVector<T>* typed) {
        CountProcessor::CountType localCount {0};
        for (const T& item : *typed) {
            localCount += item.has_value();
        }
        _count = localCount;
    }

    // e.g. COUNT(n) : count all rows
    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        _count = typed->size();
    }

    // NOTE: I think unused, but count if value is non-null
    template <TypeConcepts::OptionalType T>
    void operator()(const ColumnConst<T>* typed) {
        const T& value = typed->getRaw();
        _count = value.has_value() ? 1 : 0;
    }

    // e.g. COUNT(5) : always 1
    template <typename T>
    constexpr void operator()(const ColumnConst<T>*  /*unused*/) {
        _count = 1;
    }

    // e.g. COUNT(NULL) : count non-null values; always 0
    constexpr void operator()(const ColumnConst<PropertyNull>*  /*unused*/) {
        _count = 0;
    }
};

void CountProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();
    inputPort->consume();

    const Dataframe* inputDf = _input.getDataframe();

    // @ref _col is nullptr in the case of a COUNT(*), which in Cypher means "count the
    // number of rows in the output". Unlike a COUNT(n.name), COUNT(*) also includes
    // null-valued rows
    if (_col == nullptr) {
        const size_t blockRowCount = inputDf->getLogicalRowCount();
        _countRunning += blockRowCount;
    } else { // Otherwise, query such as COUNT(n.name)
        CountProcessor::CountType localCount {0};

        Counter counter(localCount);
        using Types = OutputtedTypes;
        using CountDispatch =
            ColumnSingleDispatcher<Types::Allowed, Counter, Types::Excluded>;

        // Updates @ref localCount in-place
        CountDispatch::dispatch(_col, counter);

        _countRunning += localCount;
    }

    // Write the count value only if the input is finished
    if (inputPort->isClosed()) {
        _countColumn->set(_countRunning);
        _output.getPort()->writeData();
        finish();
    }
}
