#include "LoadJsonlProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"
#include "ExecutionContext.h"
#include "SystemManager.h"

#include "PipelineException.h"

using namespace db;

LoadJsonlProcessor::LoadJsonlProcessor(const fs::Path& path,
                                       std::string_view graphName)
    : _path(path),
    _graphName(graphName)
{
}

LoadJsonlProcessor::~LoadJsonlProcessor() {
}

LoadJsonlProcessor* LoadJsonlProcessor::create(PipelineV2* pipeline,
                                               const fs::Path& path,
                                               std::string_view graphName) {
    LoadJsonlProcessor* proc = new LoadJsonlProcessor(path, graphName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, proc);
    proc->_outName.setPort(outName);
    proc->addOutput(outName);

    proc->postCreate(pipeline);

    return proc;
}

std::string LoadJsonlProcessor::describe() const {
    return fmt::format("LoadJsonlProcessor @={}", fmt::ptr(this));
}

void LoadJsonlProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void LoadJsonlProcessor::reset() {
}

void LoadJsonlProcessor::execute() {
    SystemManager* sysMan = _ctxt->getSystemManager();
    JobSystem* jobSystem = _ctxt->getJobSystem();

    bioassert(sysMan, "SystemManager not initialized");
    bioassert(jobSystem, "JobSystem not initialized");

    const bool res = sysMan->importGraph(std::string(_graphName), _path, *jobSystem);
    if (!res) {
        throw PipelineException(fmt::format("Failed to load neo4j graph {}", _path.get()));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* nameCol = _outName.getValue()->as<ColumnString>();
    nameCol->set(_graphName);

    _outName.getPort()->writeData();
    finish();
}
