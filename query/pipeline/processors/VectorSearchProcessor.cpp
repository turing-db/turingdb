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
    vec::VectorSearchQuery query(accessor.metadata()->_dimension);
    query.setVector(_queryVector);
    query.setMaxResultCount(_k);

    vec::VectorSearchResult result;
    const vec::VectorResult<void> searchResult = accessor.search(&query, &result);

    if (!searchResult) {
        throw PipelineException(
            fmt::format("Vector search failed: {}", searchResult.error().fmtMessage()));
    }

    // Output result IDs and scores to columns
    using ColumnInt = ColumnVector<types::Int64::Primitive>;
    ColumnInt* colIds = _outIds.getValues()->as<ColumnInt>();

    const std::span<const int64_t> ids = result.ids();
    colIds->getRaw().assign(ids.begin(), ids.end());

    // There is a single metric column: the score. The search result reports it as
    // a distance (squared L2 for EUCLID, inner product for cosine), which is the
    // value we expose to the user as the per-result score.
    using ColumnDouble = ColumnVector<types::Double::Primitive>;
    ColumnDouble* scoreColumn = _scoreColumn->as<ColumnDouble>();

    const std::span<const float> scores = result.distances();
    scoreColumn->getRaw().assign(scores.begin(), scores.end());

    _outIds.getPort()->writeData();
    finish();
}
