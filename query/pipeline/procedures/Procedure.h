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

    using ExecuteCallback = void (*)(ProcedureState&);
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

    // Arguments and return values
    const ProcedureTypeVector& returnValues() const { return _returnValues; }
    const ProcedureTypeVector& argumentTypes() const { return _argumentTypes; }

    size_t getReturnValueIndex(std::string_view name) const;
    size_t getArgumentIndex(std::string_view name) const;

    ProcedureType getReturnValueType(size_t index) const {
        return _returnValues[index]._type;
    }

    ProcedureType getArgumentType(size_t index) const {
        return _argumentTypes[index]._type;
    }

    void returnAll(std::vector<YieldItem>& yieldItems) const;

private:
    friend class ProcedureNamespace;

    std::string_view _name;
    std::string _fullName;
    ExecuteCallback _execCallback {nullptr};
    AllocCallback _allocCallback {nullptr};
    DeallocCallback _deallocCallback {nullptr};
    ProcedureTypeVector _returnValues;
    ProcedureTypeVector _argumentTypes;
};

}
