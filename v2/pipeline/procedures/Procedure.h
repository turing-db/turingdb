#pragma once

#include <memory>

#include "ProcedureBlueprint.h"
#include "ProcedureData.h"
#include "BioAssert.h"

namespace db::v2 {

class ExecutionContext;

class Procedure {
public:
    enum class Step {
        PREPARE,
        RESET,
        EXECUTE,
    };

    template <ProcedureDataType T>
    T& data() {
        msgbioassert(_data != nullptr, "Procedure data is not initialized");
        msgbioassert(dynamic_cast<T*>(_data.get()) != nullptr, "Procedure data is not of the expected type");
        return *static_cast<T*>(_data.get());
    }

    const ProcedureData& data() const { return *_data; }

    [[nodiscard]] bool isFinished() const { return _finished; }
    [[nodiscard]] Step step() const { return _step; }
    [[nodiscard]] const ExecutionContext* ctxt() const { return _ctxt; }

    void finish() { _finished = true; }

private:
    friend class DatabaseProcedureProcessor;

    std::unique_ptr<ProcedureData> _data;
    const ProcedureBlueprint* _blueprint {nullptr};
    const ExecutionContext* _ctxt {nullptr};
    bool _finished {false};
    Step _step {};
};

}
