#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db::v2 {

class ProjectionProcessor : public Processor {
public:
    static ProjectionProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

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
