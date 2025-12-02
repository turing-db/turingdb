#include "ProcedureBlueprintMap.h"

#include "ScanLabelsProcedure.h"
#include "ScanEdgeTypesProcedure.h"
#include "ScanPropertyTypesProcedure.h"

using namespace db::v2;

const std::array<ProcedureBlueprint, 3> ProcedureBlueprintMap::_blueprints {
    ScanLabelsProcedure::Blueprint,
    ScanPropertyTypesProcedure::Blueprint,
    ScanEdgeTypesProcedure::Blueprint,
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
