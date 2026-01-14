#include "CountProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "metadata/PropertyType.h"
#include "columns/ColumnDispatcher.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

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

void CountProcessor::prepare(ExecutionContext* ctxt) {
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

void CountProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();
    inputPort->consume();

    const Dataframe* inputDf = _input.getDataframe();

    if (_col == nullptr) {
        const size_t blockRowCount = inputDf->getRowCount();
        _countRunning += blockRowCount;
    } else {
        dispatchColumnVector(_col, [&](auto* col) {
            using ColType = std::remove_reference_t<decltype(*col)>;
            using ValueType = typename ColType::ValueType;

            if constexpr (is_template_t<ValueType, std::optional>) {
                for (const ValueType& v : *col) {
                    if (v.has_value()) {
                        _countRunning++;
                    }
                }
            } else {
                const size_t blockRowCount = inputDf->getRowCount();
                _countRunning += blockRowCount;
            }
        });
    }

    // Write the count value only if the input is finished
    if (inputPort->isClosed()) {
        _countColumn->set(_countRunning);
        _output.getPort()->writeData();
        finish();
    }
}
