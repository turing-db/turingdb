#include "ShowVectorIndexesProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "VectorDatabase.h"
#include "VecLibAccessor.h"
#include "PipelineException.h"

using namespace db;

ShowVectorIndexesProcessor::ShowVectorIndexesProcessor()
{
}

ShowVectorIndexesProcessor::~ShowVectorIndexesProcessor() {
}

ShowVectorIndexesProcessor* ShowVectorIndexesProcessor::create(PipelineV2* pipeline) {
    ShowVectorIndexesProcessor* proc = new ShowVectorIndexesProcessor();

    PipelineOutputPort* outNames = PipelineOutputPort::create(pipeline, proc);
    proc->_outNames.setPort(outNames);
    proc->addOutput(outNames);

    PipelineOutputPort* outDimensions = PipelineOutputPort::create(pipeline, proc);
    proc->_outDimensions.setPort(outDimensions);
    proc->addOutput(outDimensions);

    proc->postCreate(pipeline);

    return proc;
}

std::string ShowVectorIndexesProcessor::describe() const {
    return fmt::format("ShowVectorIndexesProcessor @={}", fmt::ptr(this));
}

void ShowVectorIndexesProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ShowVectorIndexesProcessor::reset() {
}

void ShowVectorIndexesProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    using ColumnString = ColumnVector<types::String::Primitive>;
    using ColumnUInt = ColumnVector<types::UInt64::Primitive>;

    ColumnString* colNames = _outNames.getValues()->as<ColumnString>();
    ColumnUInt* colDimensions = _outDimensions.getValues()->as<ColumnUInt>();

    vectorDb->listLibraryNames(_indexNames);
    for (const std::string& name : _indexNames) {
        vec::VecLibAccessor accessor = vectorDb->getLibrary(name);
        if (accessor.isValid()) {
            colNames->push_back(name);
            colDimensions->push_back(accessor.metadata()->_dimension);
        }
    }

    _outNames.getPort()->writeData();
    _outDimensions.getPort()->writeData();
    finish();
}
