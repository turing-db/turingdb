#include "ListGraphProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

using namespace db;

ListGraphProcessor::ListGraphProcessor()
{
}

ListGraphProcessor::~ListGraphProcessor() {
}

std::string ListGraphProcessor::describe() const {
    return fmt::format("ListGraphProcessor @={}", fmt::ptr(this));
}

ListGraphProcessor* ListGraphProcessor::create(PipelineV2* pipeline) {
    ListGraphProcessor* count = new ListGraphProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, count);
    count->_output.setPort(output);
    count->addOutput(output);

    count->postCreate(pipeline);
    return count;
}

void ListGraphProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ListGraphProcessor::reset() {
    markAsReset();
}

void ListGraphProcessor::execute() {
    SystemManager* sysMan = _ctxt->getSystemManager();

    using ColumnString = ColumnVector<types::String::Primitive>;
    ColumnString* colName = _output.getValue()->as<ColumnString>();
    sysMan->listGraphs(colName->getRaw());

    _output.getPort()->writeData();
    finish();
}
