#include "MaterializeProcessor.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"

using namespace db::v2;

MaterializeProcessor::MaterializeProcessor()
{
}

MaterializeProcessor::~MaterializeProcessor() {
}

MaterializeProcessor* MaterializeProcessor::create(PipelineV2* pipeline) {
    MaterializeProcessor* processor = new MaterializeProcessor();

    PipelineBuffer* input = PipelineBuffer::create(pipeline);
    processor->_input = input;
    processor->addInput(input);

    processor->postCreate(pipeline);
    return processor;
}

void MaterializeProcessor::prepare(ExecutionContext* ctxt) {
}

void MaterializeProcessor::reset() {
}

void MaterializeProcessor::execute() {
}
