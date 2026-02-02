#include "PipelineV2.h"

#include "Processor.h"
#include "PipelinePort.h"
#include "PipelineBuffer.h"
#include "processors/ExprProgram.h"

using namespace db;

PipelineV2::PipelineV2()
{
}

PipelineV2::~PipelineV2() {
    for (Processor* processor : _processors) {
        delete processor;
    }
    for (PipelineBuffer* buffer : _buffers) {
        delete buffer;
    }
    for (PipelinePort* port : _ports) {
        delete port;
    }
    for (ExprProgram* prog : _exprProgs) {
        delete prog;
    }
}

void PipelineV2::addProcessor(Processor* processor) {
    _processors.push_back(processor);
    if (processor->isSource()) {
        _sources.push_back(processor);
    }
}

void PipelineV2::addBuffer(PipelineBuffer* buffer) {
    _buffers.push_back(buffer);
}

void PipelineV2::addPort(PipelinePort* port) {
    _ports.push_back(port);
}

void PipelineV2::addExprProgram(ExprProgram* prog) {
    _exprProgs.push_back(prog);
}

void PipelineV2::clear() {
    for (Processor* processor : _processors) {
        processor->_prepared = false;
        processor->_finished = false;
        processor->_scheduled = false;
    }
}
