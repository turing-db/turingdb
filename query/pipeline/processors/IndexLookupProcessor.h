#pragma once

#include "Processor.h"

#include "interfaces/PipelineValuesInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

namespace db {

class Index;
class Column;
class PipelineV2;

template <typename Q, typename R>
class IndexLookupProcessor final : public Processor {
public:
    static IndexLookupProcessor* create(PipelineV2* pipeline, const Index* index);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValuesInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

private:
    PipelineValuesInputInterface _input;
    PipelineValuesOutputInterface _output;

    const Index* _index;

    explicit IndexLookupProcessor(const Index* index);
    ~IndexLookupProcessor() final = default;
};

}
