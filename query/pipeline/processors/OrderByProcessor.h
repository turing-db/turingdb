#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class OrderByProcessor final : public Processor {
public:
    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    OrderByProcessor();
    ~OrderByProcessor();
};

}
