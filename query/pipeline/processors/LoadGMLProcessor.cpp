#include "LoadGMLProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"
#include "BioAssert.h"

using namespace db;

LoadGMLProcessor::LoadGMLProcessor(std::string_view graphName, const fs::Path& filePath)
    : _graphName(graphName),
    _filePath(filePath)
{
}

LoadGMLProcessor::~LoadGMLProcessor() {
}

LoadGMLProcessor* LoadGMLProcessor::create(PipelineV2* pipeline, std::string_view graphName, const fs::Path& filePath) {
    LoadGMLProcessor* loadGML = new LoadGMLProcessor(graphName, filePath);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, loadGML);
    loadGML->_outName.setPort(outName);
    loadGML->addOutput(outName);

    loadGML->postCreate(pipeline);

    return loadGML;
}

std::string LoadGMLProcessor::describe() const {
    return fmt::format("LoadGMLProcessor @={}", fmt::ptr(this));
}

void LoadGMLProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void LoadGMLProcessor::reset() {
}

void LoadGMLProcessor::execute() {
    SystemManager* sysMan = _ctxt->getSystemManager();
    JobSystem* jobSys = _ctxt->getJobSystem();
    bioassert(sysMan, "SystemManager not initialised");
    bioassert(jobSys, "JobSystem not initialised");

    const bool res = sysMan->importGraph(std::string(_graphName), _filePath, *jobSys);
    if (!res) {
        throw PipelineException(fmt::format("Failed to load graph '{}'", _graphName));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_graphName);

    _outName.getPort()->writeData();
    finish();
}
