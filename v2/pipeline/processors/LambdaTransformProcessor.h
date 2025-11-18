#pragma once

#include <functional>

#include "Processor.h"

#include "PipelineInterface.h"

namespace db {
class Dataframe;
}

namespace db::v2 {

class LambdaTransformProcessor : public Processor {
public:
    enum class Operation {
        PREPARE,
        RESET,
        EXECUTE
    };

    using Callback = std::function<void(const Dataframe*,
                                        Dataframe*,
                                        bool& isFinished,
                                        Operation operation)>;

    static LambdaTransformProcessor* create(PipelineV2* pipeline, Callback callback);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    Callback _callback;
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    LambdaTransformProcessor(Callback callback);
    ~LambdaTransformProcessor();
};

}
