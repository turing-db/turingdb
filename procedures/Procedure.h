#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "ProcedureData.h"
#include "ProcedureTypeVector.h"

namespace db {

class NamedColumn;
class ProcedureState;
class ProcedureNamespace;

class Procedure {
public:
    struct Argument {
        size_t _index {0};
        const Column* _col {nullptr};
    };

    struct YieldItem {
        std::string_view _baseName;
        std::string_view _asName;
        NamedColumn* _col {nullptr};
    };

    using ExecuteCallback = void (*)(ProcedureState*);
    using AllocCallback = ProcedureData* (*)();
    using DeallocCallback = void (*)(ProcedureData*);

    Procedure(std::string_view name);
    ~Procedure();

    std::string_view getName() const { return _name; }
    const std::string& getFullName() const { return _fullName; }

    // Get callbacks
    ExecuteCallback getExecCallback() const { return _execCallback; }
    AllocCallback getAllocCallback() const { return _allocCallback; }
    DeallocCallback getDeallocCallback() const { return _deallocCallback; }

    // Set callbacks
    void setExecuteCallback(ExecuteCallback cb);
    void setAllocCallback(AllocCallback cb);
    void setDeallocCallback(DeallocCallback cb);
    void addReturnValue(std::string_view name, ProcedureType type);
    void addArgument(std::string_view name, ProcedureType type);
    void addOptionalArgument(std::string_view name, ProcedureType type);

    // Whether the procedure reports, for every row it emits, the input row it derives
    // from - ProcedureData::getInputRowIndices. Only such a procedure may be called with
    // columns carried past it: the caller has to replicate a carried row once per row
    // the procedure emitted for it, and that report is the only thing that says which
    // rows those are. Declared here so a caller can refuse the call while it is being
    // planned rather than discovering mid-execution that the rows cannot be aligned.
    //
    // False by default: a procedure emitting one row per input row still has to report
    // (there is no identity shortcut), so this is opt-in rather than assumed.
    bool reportsInputRows() const { return _reportsInputRows; }
    void setReportsInputRows(bool reportsInputRows);

    // Arguments and return values
    const ProcedureTypeVector& returnValues() const { return _returnValues; }
    const ProcedureTypeVector& argumentTypes() const { return _argumentTypes; }

    size_t getReturnValueIndex(std::string_view name) const;
    size_t getArgumentIndex(std::string_view name) const;
    size_t getRequiredArgumentCount() const;

    ProcedureType getReturnValueType(size_t index) const {
        return _returnValues[index]._type;
    }

    ProcedureType getArgumentType(size_t index) const {
        return _argumentTypes[index]._type;
    }

    void returnAll(std::vector<YieldItem>& yieldItems) const;

    bool hasIndices() const { return _hasIndices; }
    void setHasIndices(bool value) { _hasIndices = value; }

private:
    friend class ProcedureNamespace;

    std::string_view _name;
    std::string _fullName;
    ExecuteCallback _execCallback {nullptr};
    AllocCallback _allocCallback {nullptr};
    DeallocCallback _deallocCallback {nullptr};
    bool _reportsInputRows {false};
    ProcedureTypeVector _returnValues;
    ProcedureTypeVector _argumentTypes;
    bool _hasIndices {false};
};

}
