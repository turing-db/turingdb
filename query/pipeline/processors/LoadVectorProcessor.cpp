#include "LoadVectorProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "VectorDatabase.h"
#include "VecLibWriteAccessor.h"
#include "BatchVectorCreate.h"
#include "VectorCSVReader.h"
#include "PipelineException.h"
#include "VectorResult.h"

using namespace db;

LoadVectorProcessor::LoadVectorProcessor(std::string_view filePath,
                                         std::string_view indexName)
    : _filePath(filePath),
    _indexName(indexName)
{
}

LoadVectorProcessor::~LoadVectorProcessor() {
}

LoadVectorProcessor* LoadVectorProcessor::create(PipelineV2* pipeline,
                                                 std::string_view filePath,
                                                 std::string_view indexName) {
    LoadVectorProcessor* proc = new LoadVectorProcessor(filePath, indexName);

    PipelineOutputPort* outCount = PipelineOutputPort::create(pipeline, proc);
    proc->_outCount.setPort(outCount);
    proc->addOutput(outCount);

    proc->postCreate(pipeline);

    return proc;
}

std::string LoadVectorProcessor::describe() const {
    return fmt::format("LoadVectorProcessor @={}", fmt::ptr(this));
}

void LoadVectorProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void LoadVectorProcessor::reset() {
}

void LoadVectorProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    // Get write accessor for exclusive access
    vec::VecLibWriteAccessor accessor = vectorDb->getLibraryForWrite(std::string(_indexName));
    if (!accessor.isValid()) {
        throw PipelineException(fmt::format("Vector index '{}' not found", _indexName));
    }

    // Resolve file path relative to the data directory
    const SystemManager* sysMan = _ctxt->getSystemManager();
    const fs::Path& dataDir = sysMan->getConfig()->getDataDir();
    const fs::Path filePath = dataDir / std::string(_filePath);

    if (!filePath.isSubDirectory(dataDir)) {
        throw PipelineException(fmt::format(
            "Invalid file path: path must be relative to '{}'",
            dataDir.get()));
    }

    vec::BatchVectorCreate batch;
    accessor.prepareCreateBatch(&batch);

    vec::VectorCSVReader::read(filePath, batch);

    const vec::VectorResult<void> result = accessor.addEmbeddings(&batch);
    if (!result.has_value()) {
        throw PipelineException(fmt::format(
            "Failed to add embeddings: {}", result.error().fmtMessage()));
    }

    using ColumnUInt = ColumnConst<types::UInt64::Primitive>;
    ColumnUInt* colCount = _outCount.getValue()->as<ColumnUInt>();
    colCount->set(batch.count());

    _outCount.getPort()->writeData();
    finish();
}
