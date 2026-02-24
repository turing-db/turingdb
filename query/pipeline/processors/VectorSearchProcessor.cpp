#include "VectorSearchProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "VectorDatabase.h"
#include "VecLibAccessor.h"
#include "VectorSearchQuery.h"
#include "VectorSearchResult.h"
#include "PipelineException.h"
#include "VectorResult.h"

using namespace db;

VectorSearchProcessor::VectorSearchProcessor(std::string_view indexName,
                                             uint64_t k,
                                             const std::vector<float>& queryVector)
    : _indexName(indexName),
    _k(k),
    _queryVector(queryVector)
{
}

VectorSearchProcessor::~VectorSearchProcessor() {
}

VectorSearchProcessor* VectorSearchProcessor::create(PipelineV2* pipeline,
                                                     std::string_view indexName,
                                                     uint64_t k,
                                                     const std::vector<float>& queryVector) {
    VectorSearchProcessor* proc = new VectorSearchProcessor(indexName, k, queryVector);

    PipelineOutputPort* outIds = PipelineOutputPort::create(pipeline, proc);
    proc->_outIds.setPort(outIds);
    proc->addOutput(outIds);

    proc->postCreate(pipeline);

    return proc;
}

std::string VectorSearchProcessor::describe() const {
    return fmt::format("VectorSearchProcessor @={}", fmt::ptr(this));
}

void VectorSearchProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void VectorSearchProcessor::reset() {
}

void VectorSearchProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    // Get read accessor (shared lock for concurrent reads)
    vec::VecLibAccessor accessor = vectorDb->getLibrary(std::string(_indexName));
    if (!accessor.isValid()) {
        throw PipelineException(fmt::format("Vector index '{}' not found", _indexName));
    }

    // Execute search
    vec::VectorSearchQuery query(accessor.metadata()._dimension);
    query.setVector(_queryVector);
    query.setMaxResultCount(_k);

    vec::VectorSearchResult result;
    vec::VectorResult<void> searchResult = accessor.search(query, result);

    if (!searchResult) {
        throw PipelineException(
            fmt::format("Vector search failed: {}", searchResult.error().fmtMessage()));
    }

    // Output result IDs to column
    using ColumnUInt = ColumnVector<types::UInt64::Primitive>;
    ColumnUInt* colIds = _outIds.getValues()->as<ColumnUInt>();

    for (uint64_t id : result.ids()) {
        colIds->push_back(id);
    }

    _outIds.getPort()->writeData();
    finish();
}
