#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineValuesInputInterface.h"

namespace db {

class Index;
class Column;
class PipelineV2;

class IndexLookupProcessor final : public Processor {
public:
    static IndexLookupProcessor* create(PipelineV2* pipeline, const Index* index);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

private:
    PipelineValuesInputInterface _input;
    PipelineBlockOutputInterface _output;

    const Index* _index;

    explicit IndexLookupProcessor(const Index* index);
    ~IndexLookupProcessor() final = default;
};

}
