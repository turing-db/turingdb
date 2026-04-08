#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class PredicateProgram;

class FilterProcessor final : public Processor {
public:
    static FilterProcessor* create(PipelineV2* pipeline, PredicateProgram* prog);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    PredicateProgram* _prog {nullptr};

    FilterProcessor(PredicateProgram* prog);
    ~FilterProcessor() final = default ;
};

}
