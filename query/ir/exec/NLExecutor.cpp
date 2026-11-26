#include "NLExecutor.h"

#include <span>

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLSystemContext.h"

#include "NLProgram.h"
#include "NLOutputSink.h"
#include "NLLoopSteps.h"

using namespace db;

NLExecutor::NLExecutor(const GraphView* view,
                       const NLProgram* prog,
                       NLOutputSink* sink,
                       CommitWriteBuffer* writeBuffer,
                       const NLSystemContext* system,
                       MetadataBuilder* metadataBuilder)
    : _ctxt(view, sink, prog->getChunkSize(), writeBuffer, system, metadataBuilder),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    NLOutputSink* const sink = _ctxt.getSink();
    if (sink) {
        NLOutputData* const outputData = _prog->getOutputData();
        std::span<const Column* const> outputs;
        if (outputData) {
            outputs = outputData->outputs();
        }

        sink->declareOutput(_prog->columnNames(), outputs);
    }

    runBody(&_ctxt, _prog->getStmts());
}
