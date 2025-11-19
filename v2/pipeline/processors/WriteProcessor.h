#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class Dataframe;

}

namespace db::v2 {

class PipelineV2;

class WriteProcessor final : public Processor {
public:
    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final {
        return "WriteProcessor";
    }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;
};
    
}
