#pragma once

#include <functional>

#include "Processor.h"

namespace db {
class Block;
}

namespace db::v2 {

class LambdaProcessor : public Processor {
public:
    enum class Operation {
        RESET,
        EXECUTE
    };

    using Callback = std::function<void(const Block&, Operation)>;

    static LambdaProcessor* create(PipelineV2* pipeline, Callback callback);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputPort* input() const { return _input; }

private:
    Callback _callback;
    PipelineInputPort* _input {nullptr};

    LambdaProcessor(Callback callback);
    ~LambdaProcessor();
};

}
