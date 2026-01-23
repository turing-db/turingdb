#pragma once

#include <memory>
#include <vector>

#include "ProcedureData.h"
#include "procedures/ProcedureReturnValues.h"

namespace db {
class NamedColumn;
}

namespace db {

class Procedure;

class ProcedureBlueprint {
public:
    struct YieldItem {
        std::string_view _baseName;
        std::string_view _asName;
        NamedColumn* _col {nullptr};
    };

    struct InputItem {
        size_t _index {0};
        const Column* _col {nullptr};
    };

    using ExecuteCallback = void (*)(Procedure&);
    using AllocCallback = std::unique_ptr<ProcedureData> (*)();

    size_t getReturnValueIndex(std::string_view name) const;
    size_t getArgumentIndex(std::string_view name) const;

    ProcedureType getReturnValueType(size_t index) const {
        return _returnValues[index]._type;
    }

    ProcedureType getArgumentType(size_t index) const {
        return _argumentTypes[index]._type;
    }

    void returnAll(std::vector<YieldItem>& yieldItems) const;

    std::string_view _name;
    ExecuteCallback _execCallback {nullptr};
    AllocCallback _allocCallback {nullptr};
    ProcedureTypeVector _returnValues;
    ProcedureTypeVector _argumentTypes;
};

}
