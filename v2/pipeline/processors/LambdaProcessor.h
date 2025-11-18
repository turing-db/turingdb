#pragma once

#include <functional>

#include "Processor.h"

#include "PipelineInterface.h"

namespace db {
class Dataframe;
}

namespace db::v2 {

class LambdaProcessor : public Processor {
public:
    enum class Operation {
        RESET,
        EXECUTE
    };

    using Callback = std::function<void(const Dataframe*, Operation)>;

    static LambdaProcessor* create(PipelineV2* pipeline, const Callback& callback);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }

private:
    Callback _callback;
    PipelineBlockInputInterface _input;

    LambdaProcessor(const Callback& callback);
    ~LambdaProcessor();
};

}
