#include "CreateGraphProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

CreateGraphProcessor::CreateGraphProcessor(std::string_view graphName)
    : _graphName(graphName)
{
}

CreateGraphProcessor::~CreateGraphProcessor() {
}

CreateGraphProcessor* CreateGraphProcessor::create(PipelineV2* pipeline, std::string_view graphName) {
    CreateGraphProcessor* loadGraph = new CreateGraphProcessor(graphName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, loadGraph);
    loadGraph->_outName.setPort(outName);
    loadGraph->addOutput(outName);

    loadGraph->postCreate(pipeline);

    return loadGraph;
}

std::string CreateGraphProcessor::describe() const {
    return fmt::format("CreateGraphProcessor @={}", fmt::ptr(this));
}

void CreateGraphProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void CreateGraphProcessor::reset() {
}

void CreateGraphProcessor::execute() {
    SystemManager* sysMan = _ctxt->getSystemManager();

    auto* res = sysMan->createGraph(std::string(_graphName));
    if (!res) {
        throw PipelineException(fmt::format("Failed to create graph '{}'", _graphName));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_graphName);

    _outName.getPort()->writeData();
    finish();
}
