#pragma once

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db::v2 {

class ListGraphProcessor final : public Processor {
public:
    static ListGraphProcessor* create(PipelineV2* pipeline);

    std::string describe() const final;

    PipelineValueOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

private:
    PipelineValueOutputInterface _output;

    ExecutionContext* _ctxt;

    ListGraphProcessor();
    ~ListGraphProcessor();
};

}
