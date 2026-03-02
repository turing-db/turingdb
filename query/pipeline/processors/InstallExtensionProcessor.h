#pragma once

#include <string_view>

#include "Processor.h"

#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class InstallExtensionProcessor final : public Processor {
public:
    static InstallExtensionProcessor* create(PipelineV2* pipeline,
                                             std::string_view extensionName);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValueOutputInterface& output() { return _outName; }

protected:
    std::string_view _extensionName;
    PipelineValueOutputInterface _outName;

    InstallExtensionProcessor(std::string_view extensionName);
    ~InstallExtensionProcessor();
};

}
