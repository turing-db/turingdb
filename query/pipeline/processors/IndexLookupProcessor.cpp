#include "IndexLookupProcessor.h"

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"

#include "indexes/Index.h"
#include "ExecutionContext.h"

#include "dataframe/Dataframe.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

#include "BioAssert.h"

using namespace db;

template <typename Q, typename R>
IndexLookupProcessor<Q, R>::IndexLookupProcessor(const Index* index)
    : _index(index)
{
}

template <typename Q, typename R>
std::string IndexLookupProcessor<Q, R>::describe() const  {
    return fmt::format("IndexLookupProcessor @={}, Index @={}", fmt::ptr(this), fmt::ptr(_index));
}

template <typename Q, typename R>
IndexLookupProcessor<Q, R>* IndexLookupProcessor<Q, R>::create(PipelineV2* pipeline,
                                                               const Index* index) {
    IndexLookupProcessor<Q, R>* proc = new IndexLookupProcessor<Q, R>(index);

    {
        PipelineInputPort* in = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(in);
        proc->addInput(in);
    }

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

template <typename Q, typename R>
void IndexLookupProcessor<Q, R>::prepare(ExecutionContext* ctxt) {
    bioassert(_index, "Null index.");
    _ctxt = ctxt;
    markAsPrepared();
}

template <typename Q, typename R>
void IndexLookupProcessor<Q, R>::reset() {
    _state.reset();

    markAsReset();
}

template <typename Q, typename R>
void IndexLookupProcessor<Q, R>::execute() {
    const NamedColumn* queryNCol = _input.getValues();
    const NamedColumn* resultNCol = _output.getValues();
    bioassert(queryNCol && resultNCol, "Null named value columns.");

    const ColumnVector<Q>* query = queryNCol->as<ColumnVector<Q>>();
    ColumnVector<R>* result = resultNCol->as<ColumnVector<R>>();
    bioassert(query && result, "Null value columns.");

    const size_t chunkSize = _ctxt->getChunkSize();

    ColumnIndices* indices = _output.getIndices()->as<ColumnIndices>();
    bioassert(indices, "Invalid indices.");

    _index->boundedQuery(query, result, indices, _state, chunkSize);

    const bool wroteData = !result->empty();
    const bool inputClosed = _input.getPort()->isClosed();

    // Write data iff we have rows to output, or we have no rows to output but our input
    // is now closed (we will never have any more rows to write).
    if (wroteData || inputClosed) {
        _output.getPort()->writeData();
    }

    const bool finishedThisQuery = _state._finished;
    // Prepare for the next query, if we are to receive one
    if (finishedThisQuery) {
        _input.getPort()->consume();
        _state.reset();
    }

    // If we finished this query, and we are never going to receive more, finished
    if (finishedThisQuery && inputClosed) {
        finish();
    }
}

namespace db {
template class IndexLookupProcessor<types::Int64::Primitive, NodeID>;
template class IndexLookupProcessor<types::UInt64::Primitive, NodeID>;
template class IndexLookupProcessor<types::Double::Primitive, NodeID>;
template class IndexLookupProcessor<types::String::Primitive, NodeID>;
template class IndexLookupProcessor<types::Bool::Primitive, NodeID>;
template class IndexLookupProcessor<types::Embedding::Primitive, NodeID>;

template class IndexLookupProcessor<types::Int64::Primitive, EdgeID>;
template class IndexLookupProcessor<types::UInt64::Primitive, EdgeID>;
template class IndexLookupProcessor<types::Double::Primitive, EdgeID>;
template class IndexLookupProcessor<types::String::Primitive, EdgeID>;
template class IndexLookupProcessor<types::Bool::Primitive, EdgeID>;
template class IndexLookupProcessor<types::Embedding::Primitive, EdgeID>;
}

