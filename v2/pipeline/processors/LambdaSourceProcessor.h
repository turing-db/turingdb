#pragma once

#include <functional>

#include "Processor.h"

namespace db {
class Block;
}

namespace db::v2 {

class LambdaSourceProcessor : public Processor {
public:
    enum class Operation {
        PREPARE,
        RESET,
        EXECUTE
    };

    using Callback = std::function<void(Block&, bool& isFinished, Operation operation)>;

    static LambdaSourceProcessor* create(PipelineV2* pipeline, Callback callback);

    std::string_view getName() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineOutputPort* output() const { return _output; }

private:
    Callback _callback;
    PipelineOutputPort* _output {nullptr};

    LambdaSourceProcessor(Callback callback);
    ~LambdaSourceProcessor();
};

}
