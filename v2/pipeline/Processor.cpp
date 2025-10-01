#include "Processor.h"

#include "PipelineV2.h"

using namespace db::v2;

Processor::Processor()
{
}

Processor::~Processor() {
}

void Processor::postCreate(PipelineV2* pipeline) {
    pipeline->addProcessor(this);
}

void Processor::addInput(PipelineInputPort* buffer) {
    _inputs.push_back(buffer);
}

void Processor::addOutput(PipelineOutputPort* buffer) {
    _outputs.push_back(buffer);
}

void Processor::finish() {
    _finished = true;

    const bool inputsClosed = checkInputsClosed();
    if (inputsClosed) {
        closeOutputs();
    }
}

bool Processor::checkInputsClosed() const {
    for (const PipelineInputPort* input : _inputs) {
        if (!input->isClosed()) {
            return false;
        }
    }
    return true;
}

void Processor::closeOutputs() {
    for (PipelineOutputPort* output : _outputs) {
        output->close();
    }
}
