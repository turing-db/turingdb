#include "IndexLookupProcessor.h"

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"

#include "indexes/Index.h"

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
void IndexLookupProcessor<Q, R>::prepare(ExecutionContext*) {
    bioassert(_index, "Null index.");
    markAsPrepared();
}

template <typename Q, typename R>
void IndexLookupProcessor<Q, R>::reset() {
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

    _index->query(query, result);
    _output.getPort()->writeData();

    // TODO: Chunk
    finish();
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

