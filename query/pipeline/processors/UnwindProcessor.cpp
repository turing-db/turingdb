#include "UnwindProcessor.h"

#include <range/v3/view/drop.hpp>
#include <range/v3/view/take.hpp>

#include "ExecutionContext.h"

#include "ListElementView.h"

#include "columns/Column.h"
#include "columns/ColumnVector.h"

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelinePort.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

UnwindProcessor::UnwindProcessor(ListView list)
    : _list(list)
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

void UnwindProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void UnwindProcessor::reset() {
    _index = 0;
    markAsReset();
}

void UnwindProcessor::execute() {
    bioassert(_list, "Invalid list in UNWIND.");

    const NamedColumn* values = _output.getValues();
    Column* valueCol = values->getColumn();

    auto* typedCol = dynamic_cast<ColumnVector<ListElementView>*>(valueCol);
    bioassert(typedCol, "Invalid column to UNWIND.");

    // Get at most a chunks worth of elements, starting from @ref _index
    const auto toEmit = _list | rv::drop(_index) | rv::take(_ctxt->getChunkSize());
    const size_t count = toEmit.size();

    typedCol->resize(count);
    std::ranges::copy(toEmit, typedCol->begin());
    _output.getPort()->writeData();

    _index += count;

    const bool finished = _index == _list.size();
    if (finished) {
        finish();
    }
}
