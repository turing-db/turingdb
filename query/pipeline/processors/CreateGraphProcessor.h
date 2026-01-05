#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class CreateGraphProcessor final : public Processor {
public:
    static CreateGraphProcessor* create(PipelineV2* pipeline, std::string_view graphName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _graphName;
    PipelineValueOutputInterface _outName;

    CreateGraphProcessor(std::string_view graphName);
    ~CreateGraphProcessor();
};

}
