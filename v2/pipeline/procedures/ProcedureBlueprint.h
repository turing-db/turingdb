#pragma once

#include <memory>
#include <vector>

#include "ProcedureData.h"
#include "procedures/ProcedureReturnValues.h"

#include "PipelineException.h"
#include "BioAssert.h"

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

    size_t getReturnValueIndex(std::string_view name) const {
        for (size_t i = 0; i < _returnValues.size(); ++i) {
            if (_returnValues[i]._name == name) {
                return i;
            }
        }

        throw PipelineException("Column is not returned by the procedure");
    }

    ProcedureReturnType getReturnValueType(size_t index) const {
        return _returnValues[index]._type;
    }

    std::string_view _name;
    ExecuteCallback _execCallback = nullptr;
    AllocCallback _allocCallback = nullptr;
    ProcedureReturnValues _returnValues;
    bool _valid = false;

    void returnAll(std::vector<YieldItem>& yieldItems) const {
        for (const auto& item : _returnValues) {
            msgbioassert(item._type != ProcedureReturnType::INVALID,
                         "Invalid procedure return type");

            yieldItems.emplace_back(item._name);
        }
    }
};

}
