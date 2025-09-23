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

void Processor::addInput(PipelinePort* buffer) {
    _inputs.push_back(buffer);
}

void Processor::addOutput(PipelinePort* buffer) {
    _outputs.push_back(buffer);
}
