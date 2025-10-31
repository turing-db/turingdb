#pragma once

#include <functional>

#include "Processor.h"

#include "PipelineInterface.h"

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

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineOutputInterface& output() { return _output; }

private:
    Callback _callback;
    PipelineOutputInterface _output;

    LambdaSourceProcessor(Callback callback);
    ~LambdaSourceProcessor();
};

}
