#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class LoadCommitProcessor : public Processor {
public:
    static LoadCommitProcessor* create(PipelineV2* pipeline, std::string_view hashStr);

    std::string describe() const override;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockOutputInterface _output;
    std::string_view _hashStr;

    ExecutionContext* _ctxt {nullptr};

    LoadCommitProcessor(std::string_view hashStr);
    ~LoadCommitProcessor() override;
};

}
