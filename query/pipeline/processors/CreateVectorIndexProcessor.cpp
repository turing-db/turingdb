#include "CreateVectorIndexProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "VectorDatabase.h"
#include "PipelineException.h"
#include "VectorResult.h"
#include "VecLibMetadata.h"

using namespace db;

CreateVectorIndexProcessor::CreateVectorIndexProcessor(std::string_view indexName,
                                                       vec::Dimension dimension,
                                                       vec::DistanceMetric metric,
                                                       vec::IndexType indexType)
    : _indexName(indexName),
    _dimension(dimension),
    _metric(metric),
    _indexType(indexType)
{
}

CreateVectorIndexProcessor::~CreateVectorIndexProcessor() {
}

CreateVectorIndexProcessor* CreateVectorIndexProcessor::create(PipelineV2* pipeline,
                                                               std::string_view indexName,
                                                               vec::Dimension dimension,
                                                               vec::DistanceMetric metric,
                                                               vec::IndexType indexType) {
    CreateVectorIndexProcessor* proc = new CreateVectorIndexProcessor(indexName, dimension, metric, indexType);

    PipelineOutputPort* outName = PipelineOutputPort::create(pipeline, proc);
    proc->_outName.setPort(outName);
    proc->addOutput(outName);

    proc->postCreate(pipeline);

    return proc;
}

std::string CreateVectorIndexProcessor::describe() const {
    return fmt::format("CreateVectorIndexProcessor @={}", fmt::ptr(this));
}

void CreateVectorIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void CreateVectorIndexProcessor::reset() {
}

void CreateVectorIndexProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    const vec::VectorResult<vec::VecLibID> result =
        vectorDb->createLibrary(std::string(_indexName),
                                _dimension,
                                _metric,
                                _indexType);

    if (!result.has_value()) {
        throw PipelineException(fmt::format("Failed to create vector index '{}': {}",
                                            _indexName, result.error().fmtMessage()));
    }

    using ColumnString = ColumnConst<types::String::Primitive>;
    ColumnString* colName = _outName.getValue()->as<ColumnString>();
    colName->set(_indexName);

    _outName.getPort()->writeData();
    finish();
}
