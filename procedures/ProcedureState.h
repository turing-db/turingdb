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

    // Set up the state before the first callback: the data the alloc callback
    // produced, the procedure whose callbacks run against it, and the execution
    // context they read the graph through. Together with setStep and
    // clearFinished, these let a driver other than CallProcedureProcessor - the
    // MLIR engine's nl.procedure statements - own a procedure's state.
    void setData(ProcedureData* data) { _data = data; }
    void setProcedure(const Procedure* procedure) { _procedure = procedure; }
    void setContext(const ProcedureContext* ctxt) { _ctxt = ctxt; }

    // The step the next callback runs as. A driver sets it before each callback,
    // so the procedure's single execute entry point knows which phase it is in.
    void setStep(Step step) { _step = step; }

    // Clear the finished flag, so the procedure is driven again. A per-chunk
    // driver calls this before each execute: a procedure that produces all its
    // rows for one chunk in a single call marks itself finished, and the next
    // chunk must not be skipped because of it.
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
