#pragma once

#include "ChangeOp.h"
#include "Processor.h"

#include "columns/ColumnVector.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "versioning/ChangeID.h"

namespace db::v2 {

class ChangeProcessor : public Processor {
public:
    static ChangeProcessor* create(PipelineV2* pipeline, ChangeOp op);

    std::string describe() const override;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    void setColumn(ColumnVector<ChangeID>* changeIDCol) {
        _changeIDCol = changeIDCol;
    }

private:
    PipelineBlockOutputInterface _output;

    ExecutionContext* _ctxt {nullptr};
    ChangeOp _op {};
    ColumnVector<ChangeID>* _changeIDCol {nullptr};

    explicit ChangeProcessor(ChangeOp op);
    ~ChangeProcessor() override;

    void createChange() const;
    void submitChange() const;
    void deleteChange() const;
    void listChanges() const;
};

}
