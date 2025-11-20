#include "WriteProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

using namespace db::v2;

WriteProcessor* WriteProcessor::create(PipelineV2 *pipeline) {
    auto* processor = new WriteProcessor();

    {
        auto* inputPort = PipelineInputPort::create(pipeline, processor);
        processor->_input.setPort(inputPort);
    }

    {
        auto* outputPort = PipelineOutputPort::create(pipeline, processor);
        processor->_output.setPort(outputPort);
    }

    processor->postCreate(pipeline);
    return processor;
}
