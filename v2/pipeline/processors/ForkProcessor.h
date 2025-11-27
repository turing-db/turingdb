#pragma once

#include "Processor.h"
#include "interfaces/PipelineBlockInputInterface.h"

namespace db::v2 {
class PipelineBlockOutputInterface;
class PipelineBlockInputInterface;

class ForkProcessor : public Processor {
public:
    static ForkProcessor* create(PipelineV2* pipeline, size_t count);

    PipelineBlockInputInterface& input() { return _input; }
    std::vector<PipelineBlockOutputInterface>& outputs() { return _outputs; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    std::string describe() const final;

private:
    PipelineBlockInputInterface _input;
    std::vector<PipelineBlockOutputInterface> _outputs;

    explicit ForkProcessor(uint8_t count);
    ForkProcessor() = delete;
};
}
