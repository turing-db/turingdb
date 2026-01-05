#include "ForkProcessor.h"

#include "spdlog/fmt/bundled/format.h"

#include "dataframe/Dataframe.h"
#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineInputInterface.h"

using namespace db::v2;

std::string ForkProcessor::describe() const {
    return fmt::format("ForkProcessor");
}

ForkProcessor::ForkProcessor(size_t count)
    : _outputs(count)
{
}

ForkProcessor* ForkProcessor::create(PipelineV2* pipeline,
                                     size_t count) {
    ForkProcessor* fork = new ForkProcessor(count);
    PipelineInputPort* input = PipelineInputPort::create(pipeline, fork);
    fork->_input.setPort(input);
    fork->addInput(input);

    for (size_t i = 0; i < fork->_outputs.size(); ++i) {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, fork);
        fork->_outputs[i].setPort(output);
        fork->addOutput(output);
    }

    fork->postCreate(pipeline);

    return fork;
}

void ForkProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ForkProcessor::reset() {
    markAsReset();
}

void ForkProcessor::execute() {
    bool isFinished = true;
    for (const auto& output : _outputs) {
        const auto outputRes = output.getPort()->getConnectedPort()->getProcessor()->isFinished();
        isFinished &= outputRes;
    }

    if (isFinished) {
        finish();
        return;
    }

    for (const auto& output : _outputs) {
        // Copy the dataframes over - sharing buffers gets messy
        // without having a specialised port.
        output.getDataframe()->copyFrom(input().getDataframe());
        // Every port can write data
        output.getPort()->writeData();
    }
}
