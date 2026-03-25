#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class PipelineV2;
class ExecutionContext;

class CreateNodePropertyIndexProcessor final : public Processor {
public:
    static CreateNodePropertyIndexProcessor* create(PipelineV2* pipeline,
                                                std::string_view indexName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockOutputInterface& output() { return _output; }

protected:
    std::string_view _indexName;

    PipelineBlockOutputInterface _output;

    ExecutionContext* _ctxt {nullptr};

    explicit CreateNodePropertyIndexProcessor(std::string_view indexName);
    ~CreateNodePropertyIndexProcessor() final = default;
};

}
