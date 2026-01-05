#include "ProcedureBlueprint.h"

#include "PipelineException.h"

#include "BioAssert.h"

using namespace db::v2;

size_t ProcedureBlueprint::getReturnValueIndex(std::string_view name) const {
    for (size_t i = 0; i < _returnValues.size(); ++i) {
        if (_returnValues[i]._name == name) {
            return i;
        }
    }

    throw PipelineException("Column is not returned by the procedure");
}

void ProcedureBlueprint::returnAll(std::vector<YieldItem>& yieldItems) const {
    for (const auto& item : _returnValues) {
        bioassert(item._type != ProcedureReturnType::INVALID,
                  "Invalid procedure return type");

        yieldItems.emplace_back(item._name);
    }
}
