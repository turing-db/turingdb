#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ProcedureBlueprintMap;
class NamedColumn;

class ShowProceduresProcessor final : public Processor {
public:
    static ShowProceduresProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    void setNameColumn(NamedColumn* col) { _nameCol = col; }
    void setSignatureColumn(NamedColumn* col) { _signatureCol = col; }

private:
    const ExecutionContext* _ctxt {nullptr};
    PipelineBlockOutputInterface _output;
    NamedColumn* _nameCol {nullptr};
    NamedColumn* _signatureCol {nullptr};

    ShowProceduresProcessor();
    ~ShowProceduresProcessor();
};

}
