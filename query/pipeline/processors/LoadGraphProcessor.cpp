#include "LoadGraphProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"

#include "dump/LoadGraphResult.h"

using namespace db;

LoadGraphProcessor::LoadGraphProcessor(std::string_view graphName)
    : _graphName(graphName)
{
}

LoadGraphProcessor::~LoadGraphProcessor() {
}

LoadGraphProcessor* LoadGraphProcessor::create(PipelineV2* pipeline, std::string_view graphName) {
    LoadGraphProcessor* loadGraph = new LoadGraphProcessor(graphName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, loadGraph);
    loadGraph->_outName.setPort(outName);
    loadGraph->addOutput(outName);

    loadGraph->postCreate(pipeline);

    return loadGraph;
}

std::string LoadGraphProcessor::describe() const {
    return fmt::format("LoadGraphProcessor @={}", fmt::ptr(this));
}

void LoadGraphProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void LoadGraphProcessor::reset() {
}

void LoadGraphProcessor::execute() {
    SystemAccessor* system = _ctxt->getSystemAccessor();

    const LoadGraphResult<Graph*> res = system->loadGraph(_graphName);
    if (!res) {
        throw PipelineException(
            fmt::format("Failed to load graph '{}': {}", _graphName, res.error().fmtMessage()));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_graphName);

    _outName.getPort()->writeData();
    finish();
}
