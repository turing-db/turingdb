#pragma once

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class ProjectionProcessor : public Processor {
public:
    static ProjectionProcessor* create(PipelineV2* pipeline);

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    ProjectionProcessor();
    ~ProjectionProcessor();
};

}
