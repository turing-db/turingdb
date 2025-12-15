#include "LoadNeo4jProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db::v2;
using namespace db;

LoadNeo4jProcessor::LoadNeo4jProcessor(const fs::Path& path,
                                       std::string_view graphName)
    : _path(path),
      _graphName(graphName) {
}

LoadNeo4jProcessor::~LoadNeo4jProcessor() {
}

LoadNeo4jProcessor* LoadNeo4jProcessor::create(PipelineV2* pipeline,
                                               const fs::Path& path,
                                               std::string_view graphName) {
    LoadNeo4jProcessor* proc = new LoadNeo4jProcessor(path, graphName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, proc);
    proc->_outName.setPort(outName);
    proc->addOutput(outName);

    proc->postCreate(pipeline);

    return proc;
}

std::string LoadNeo4jProcessor::describe() const {
    return fmt::format("LoadNeo4jProcessor @={}", fmt::ptr(this));
}

void LoadNeo4jProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void LoadNeo4jProcessor::reset() {
}

void LoadNeo4jProcessor::execute() {
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
