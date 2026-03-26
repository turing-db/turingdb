#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class PipelineV2;
class ExecutionContext;
class CommitBuilder;

class CreateNodePropertyIndexProcessor final : public Processor {
public:
    static CreateNodePropertyIndexProcessor* create(PipelineV2* pipeline,
                                                    std::string_view indexName,
                                                    std::string_view propName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockOutputInterface& output() { return _output; }

protected:
    ExecutionContext* _ctxt {nullptr};
    CommitBuilder* _commitBuilder {nullptr};

    PipelineBlockOutputInterface _output;

    std::string_view _indexName;
    std::string_view _propertyName;

    explicit CreateNodePropertyIndexProcessor(std::string_view indexName, std::string_view propName);
    ~CreateNodePropertyIndexProcessor() final = default;
};

}
