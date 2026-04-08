#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class PipelineV2;
class ExecutionContext;
class CommitBuilder;

class DropIndexProcessor final : public Processor {
public:
    static DropIndexProcessor* create(PipelineV2* pipeline, std::string_view indexName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockOutputInterface& output() { return _output; }

private:
    ExecutionContext* _ctxt {nullptr};
    CommitBuilder* _commitBuilder {nullptr};

    PipelineBlockOutputInterface _output;

    std::string_view _indexName;

    explicit DropIndexProcessor(std::string_view indexName);
    ~DropIndexProcessor() final = default;
};

}
