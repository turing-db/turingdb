#pragma once

#include "Processor.h"

namespace db::v2 {

class MaterializeProcessor : public Processor {
public:
    static MaterializeProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;

    void reset() override;

    void execute() override;

private:
    PipelineBuffer* _input {nullptr};

    MaterializeProcessor();
    ~MaterializeProcessor();
};

}
