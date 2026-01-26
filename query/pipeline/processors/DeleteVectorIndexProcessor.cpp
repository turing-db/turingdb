#include "DeleteVectorIndexProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "VectorDatabase.h"
#include "PipelineException.h"
#include "VectorResult.h"

using namespace db;

DeleteVectorIndexProcessor::DeleteVectorIndexProcessor(std::string_view indexName)
    : _indexName(indexName)
{
}

DeleteVectorIndexProcessor::~DeleteVectorIndexProcessor() {
}

DeleteVectorIndexProcessor* DeleteVectorIndexProcessor::create(PipelineV2* pipeline,
                                                               std::string_view indexName) {
    DeleteVectorIndexProcessor* proc = new DeleteVectorIndexProcessor(indexName);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, proc);
    proc->_outName.setPort(outName);
    proc->addOutput(outName);

    proc->postCreate(pipeline);

    return proc;
}

std::string DeleteVectorIndexProcessor::describe() const {
    return fmt::format("DeleteVectorIndexProcessor @={}", fmt::ptr(this));
}

void DeleteVectorIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void DeleteVectorIndexProcessor::reset() {
}

void DeleteVectorIndexProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    const vec::VectorResult<void> result = vectorDb->deleteLibrary(std::string(_indexName));

    if (!result.has_value()) {
        throw PipelineException(fmt::format("Failed to delete vector index '{}': {}",
                                            _indexName, result.error().fmtMessage()));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_indexName);

    _outName.getPort()->writeData();
    finish();
}
