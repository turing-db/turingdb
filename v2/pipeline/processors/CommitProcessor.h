#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db::v2 {

class CommitProcessor : public Processor {
public:
    static CommitProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockOutputInterface _output;

    ExecutionContext* _ctxt {nullptr};

    CommitProcessor();
    ~CommitProcessor() override;
};

}
