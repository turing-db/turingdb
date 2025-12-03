#pragma once

#include <memory>
#include <vector>

#include "ProcedureData.h"
#include "procedures/ProcedureReturnValues.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class Procedure;

class ProcedureBlueprint {
public:
    struct YieldItem {
        std::string_view _baseName;
        std::string_view _asName;
        NamedColumn* _col {nullptr};
    };

    using ExecuteCallback = void (*)(Procedure&);
    using AllocCallback = std::unique_ptr<ProcedureData> (*)();

    size_t getReturnValueIndex(std::string_view name) const;

    ProcedureReturnType getReturnValueType(size_t index) const {
        return _returnValues[index]._type;
    }

    void returnAll(std::vector<YieldItem>& yieldItems) const;

    std::string_view _name;
    ExecuteCallback _execCallback = nullptr;
    AllocCallback _allocCallback = nullptr;
    ProcedureReturnValues _returnValues;
    bool _valid = false;
};

}
