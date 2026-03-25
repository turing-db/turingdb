#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class PipelineV2;
class ExecutionContext;

class CreatePropertyIndexProcessor final : public Processor {

    static CreatePropertyIndexProcessor* create(PipelineV2* pipeline,
                                                std::string_view propertyName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

protected:
    std::string_view _propertyName;

    PipelineBlockOutputInterface _output;

    ExecutionContext* _ctxt {nullptr};

    explicit CreatePropertyIndexProcessor(std::string_view propertyName);
    ~CreatePropertyIndexProcessor() final = default;
};

}
