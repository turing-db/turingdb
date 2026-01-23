#include "ProcedureBlueprint.h"

#include "PipelineException.h"

#include "BioAssert.h"

using namespace db;

size_t ProcedureBlueprint::getReturnValueIndex(std::string_view name) const {
    for (size_t i = 0; i < _returnValues.size(); ++i) {
        if (_returnValues[i]._name == name) {
            return i;
        }
    }

    throw PipelineException("Column is not returned by the procedure");
}

size_t ProcedureBlueprint::getArgumentIndex(std::string_view name) const {
    for (size_t i = 0; i < _argumentTypes.size(); ++i) {
        if (_argumentTypes[i]._name == name) {
            return i;
        }
    }

    throw PipelineException("Column is not an argument of the procedure");
}

void ProcedureBlueprint::returnAll(std::vector<YieldItem>& yieldItems) const {
    for (const auto& item : _returnValues) {
        bioassert(item._type != ProcedureType::INVALID,
                  "Invalid procedure return type");

        yieldItems.emplace_back(item._name);
    }
}
