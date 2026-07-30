#pragma once

#include "Procedure.h"
#include "ProcedureData.h"

#include "BioAssert.h"

namespace db {

class ProcedureContext;

class ProcedureState {
public:
    enum class Step {
        PREPARE,
        RESET,
        EXECUTE,
    };

    template <ProcedureDataType T>
    T& data() {
        bioassert(_data != nullptr, "Procedure data is not initialized");
        bioassert(dynamic_cast<T*>(_data) != nullptr,
                 "Procedure data is not of the expected type");
        return *static_cast<T*>(_data);
    }

    const ProcedureData& getData() const { return *_data; }

    const ProcedureContext* getContext() const { return _ctxt; }

    bool isFinished() const { return _finished; }

    Step getStep() const { return _step; }

    void finish() { _finished = true; }

    void setData(ProcedureData* data) { _data = data; }
    void setProcedure(const Procedure* procedure) { _procedure = procedure; }
    void setContext(const ProcedureContext* ctxt) { _ctxt = ctxt; }

    void setStep(Step step) { _step = step; }

    void clearFinished() { _finished = false; }

private:
    friend class CallProcedureProcessor;

    ProcedureData* _data {nullptr};
    const Procedure* _procedure {nullptr};
    const ProcedureContext* _ctxt {nullptr};
    bool _finished {false};
    Step _step {Step::PREPARE};
};

}
