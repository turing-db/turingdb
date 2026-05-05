#include "UnwindProcessor.h"

#include <range/v3/view/drop.hpp>
#include <range/v3/view/take.hpp>

#include "ExecutionContext.h"

#include "ListBufferTypeTag.h"
#include "ListElementView.h"
#include "ListUtils.h"

#include "columns/Column.h"
#include "columns/ColumnVector.h"

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelinePort.h"

#include "metadata/PropertyType.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

UnwindProcessor::UnwindProcessor(ListView list)
    : _list(list)
{
}

UnwindProcessor::UnwindProcessor(ListView list, ValueType homogeneity)
    : _list(list),
    _homogeneity(homogeneity)
{
}

UnwindProcessor::~UnwindProcessor() {
}

UnwindProcessor* UnwindProcessor::create(PipelineV2* pipeline, ListView list) {
    auto* proc = new UnwindProcessor(list);

    {
        PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(outPort);
        proc->addOutput(outPort);
    }

    proc->postCreate(pipeline);

    return proc;
}

UnwindProcessor* UnwindProcessor::create(PipelineV2* pipeline, ListView list, ValueType homogeneity) {
    auto* proc = new UnwindProcessor(list, homogeneity);

    {
        PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(outPort);
        proc->addOutput(outPort);
    }

    proc->postCreate(pipeline);

    return proc;
}

void UnwindProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    _isHomogeneous = _homogeneity.has_value();

    markAsPrepared();
}

void UnwindProcessor::reset() {
    _index = 0;
    markAsReset();
}

void UnwindProcessor::fillHeterogeneous(Column* outCol) {
    auto* typedCol = dynamic_cast<ColumnVector<ListElementView>*>(outCol);
    bioassert(typedCol, "Invalid column to UNWIND.");

    // Get at most a chunks worth of elements, starting from @ref _index
    const auto toEmit = _list | rv::drop(_index) | rv::take(_ctxt->getChunkSize());
    const size_t count = toEmit.size();

    typedCol->resize(count);
    std::ranges::copy(toEmit, typedCol->begin());
    _output.getPort()->writeData();

    _index += count;
}

void UnwindProcessor::fillHomogeneous(Column* outCol) {
    bioassert(_isHomogeneous, "Non-homogeneous UNWIND.");

    const auto fill = [&]<SupportedType T>() {
        using Primitive = T::Primitive;

        auto* typedCol = dynamic_cast<ColumnVector<Primitive>*>(outCol);
        bioassert(typedCol, "Invalid column.");

        // Get at most a chunks worth of elements, starting from @ref _index
        const auto toEmit = _list | rv::drop(_index) | rv::take(_ctxt->getChunkSize());
        const size_t count = toEmit.size();

        typedCol->resize(count);

        for (size_t i = 0; const ListElementView item : toEmit) {
            const ListBufferTypeTag tag = item.getTag();

            constexpr ListBufferTypeTag expectedTag = TypeToListBufferTag<Primitive>::Tag;
            bioassert(expectedTag == tag, "Invalid element.");

            const Primitive v = item.getAs<Primitive>();
            typedCol->operator[](i) = v;
            i++;
        }

        _output.getPort()->writeData();
        _index += count;
    };

    const ValueType type = homogeneity();
    ValueTypeDispatcher {type}.execute(fill);
}

void UnwindProcessor::execute() {
    bioassert(_list, "Invalid list in UNWIND.");

    const NamedColumn* values = _output.getValues();
    Column* valueCol = values->getColumn();

    _isHomogeneous ? fillHomogeneous(valueCol) : fillHeterogeneous(valueCol);

    const bool finished = _index == _list.size();
    if (finished) {
        finish();
    }
}
