#pragma once

#include "Procedure.h"
#include "ProcedureData.h"

#include "BioAssert.h"

namespace db {

class ExecutionContext;

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
        bioassert(dynamic_cast<T*>(_data) != nullptr, "Procedure data is not of the expected type");
        return *static_cast<T*>(_data);
    }

    const ProcedureData& getData() const { return *_data; }

    [[nodiscard]] const ExecutionContext* getContext() const { return _ctxt; }

    [[nodiscard]] bool isFinished() const { return _finished; }

    [[nodiscard]] Step getStep() const { return _step; }

    void finish() { _finished = true; }

private:
    friend class CallProcedureProcessor;

    ProcedureData* _data {nullptr};
    const Procedure* _procedure {nullptr};
    const ExecutionContext* _ctxt {nullptr};
    bool _finished {false};
    Step _step {Step::PREPARE};
};

}
