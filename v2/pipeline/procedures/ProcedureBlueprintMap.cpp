#include "ProcedureBlueprintMap.h"

#include "LabelsProcedure.h"
#include "EdgeTypesProcedure.h"
#include "PropertyTypesProcedure.h"

using namespace db::v2;

const std::array<ProcedureBlueprint, 3> ProcedureBlueprintMap::_blueprints {
    LabelsProcedure::Blueprint,
    PropertyTypesProcedure::Blueprint,
    EdgeTypesProcedure::Blueprint,
};

ProcedureBlueprintMap::ProcedureBlueprintMap() {
}

ProcedureBlueprintMap::~ProcedureBlueprintMap() {
}

const ProcedureBlueprint* ProcedureBlueprintMap::getBlueprint(const std::string_view& name) {
    for (const auto& blueprint : _blueprints) {
        if (blueprint._name == name) {
            return &blueprint;
        }
    }

    return nullptr;
}
