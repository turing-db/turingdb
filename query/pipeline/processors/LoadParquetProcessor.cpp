#include "LoadParquetProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/NamedColumn.h"
#include "columns/ColumnConst.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "PipelineException.h"
#include "BioAssert.h"

using namespace db;

LoadParquetProcessor::LoadParquetProcessor(std::string_view graphName, const fs::Path& filePath)
    : _graphName(graphName),
    _filePath(filePath)
{
}

LoadParquetProcessor::~LoadParquetProcessor() {
}

LoadParquetProcessor* LoadParquetProcessor::create(PipelineV2* pipeline, std::string_view graphName, const fs::Path& filePath) {
    LoadParquetProcessor* loadParquet = new LoadParquetProcessor(graphName, filePath);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, loadParquet);
    loadParquet->_outName.setPort(outName);
    loadParquet->addOutput(outName);

    loadParquet->postCreate(pipeline);

    return loadParquet;
}

std::string LoadParquetProcessor::describe() const {
    return fmt::format("LoadParquetProcessor @={}", fmt::ptr(this));
}

void LoadParquetProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void LoadParquetProcessor::reset() {
}

void LoadParquetProcessor::execute() {
    SystemAccessor* system = _ctxt->getSystemAccessor();
    bioassert(system, "SystemAccessor not initialised");

    Graph* graph = system->importGraph(_filePath, _graphName);
    if (!graph) {
        throw PipelineException(fmt::format("Failed to load graph '{}'", _graphName));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_graphName);

    _outName.getPort()->writeData();
    finish();
}
