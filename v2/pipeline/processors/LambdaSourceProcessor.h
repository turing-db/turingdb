#pragma once

#include <functional>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {
class Dataframe;
}

namespace db::v2 {

class LambdaSourceProcessor : public Processor {
public:
    enum class Operation {
        PREPARE,
        RESET,
        EXECUTE
    };

    using Callback = std::function<void(Dataframe*, bool& isFinished, Operation operation)>;

    static LambdaSourceProcessor* create(PipelineV2* pipeline, Callback callback);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockOutputInterface& output() { return _output; }

private:
    Callback _callback;
    PipelineBlockOutputInterface _output;

    LambdaSourceProcessor(Callback callback);
    ~LambdaSourceProcessor();
};

}
