#pragma once

#include "Processor.h"

#include "ListView.h"
#include "interfaces/PipelineValuesOutputInterface.h"

namespace db {

class UnwindProcessor final : public Processor {
public:
    static UnwindProcessor* create(PipelineV2* pipeline, ListView list);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;
    std::string describe() const final { return "UnwindProcessor"; }

    ListView list() const { return _list; }

    PipelineValuesOutputInterface& output() { return _output; }

private:
    explicit UnwindProcessor(ListView list);
    ~UnwindProcessor() final;
    
    PipelineValuesOutputInterface _output;

    ListView _list;
};

}
