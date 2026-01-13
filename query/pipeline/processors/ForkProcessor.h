#pragma once

#include "Processor.h"
#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ForkProcessor : public Processor {
public:
    static ForkProcessor* create(PipelineV2* pipeline, size_t count);

    PipelineBlockInputInterface& input() { return _input; }
    std::vector<PipelineBlockOutputInterface>& outputs() { return _outputs; }

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

private:
    PipelineBlockInputInterface _input;
    std::vector<PipelineBlockOutputInterface> _outputs;

    explicit ForkProcessor(size_t count);
    ForkProcessor() = delete;
};
}
