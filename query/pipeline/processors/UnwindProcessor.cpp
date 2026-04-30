#include "UnwindProcessor.h"

#include "ListElementView.h"

#include "columns/Column.h"
#include "columns/ColumnVector.h"

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "PipelinePort.h"

#include "BioAssert.h"

using namespace db;

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

void UnwindProcessor::prepare(ExecutionContext*) {
    markAsPrepared();
}

void UnwindProcessor::reset() {
    markAsReset();
}

void UnwindProcessor::execute() {
    bioassert(_list, "Invalid list in UNWIND.");

    const NamedColumn* values = _output.getValues();
    Column* valueCol = values->getColumn();

    auto* typedCol = dynamic_cast<ColumnVector<ListElementView>*>(valueCol);
    bioassert(typedCol, "Invalid column to UNWIND.");

    for (const ListElementView item : _list) {
        typedCol->push_back(item);
    }

    _output.getPort()->writeData();
    finish();
}
