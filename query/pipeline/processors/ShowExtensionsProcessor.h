#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ExtensionManager;
class NamedColumn;

class ShowExtensionsProcessor final : public Processor {
public:
    static ShowExtensionsProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    void setNameColumn(NamedColumn* col) { _nameCol = col; }

private:
    const ExecutionContext* _ctxt {nullptr};
    PipelineBlockOutputInterface _output;
    NamedColumn* _nameCol {nullptr};

    ShowExtensionsProcessor();
    ~ShowExtensionsProcessor();
};

}
