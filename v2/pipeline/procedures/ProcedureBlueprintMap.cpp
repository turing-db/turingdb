#include "ProcedureBlueprintMap.h"

#include "LabelsProcedure.h"
#include "EdgeTypesProcedure.h"
#include "PropertyTypesProcedure.h"

#include "BioAssert.h"

using namespace db::v2;

const std::array<ProcedureBlueprint, 3> ProcedureBlueprintMap::_blueprints {
    LabelsProcedure::createBlueprint(),
    PropertyTypesProcedure::createBlueprint(),
    EdgeTypesProcedure::createBlueprint(),
};

ProcedureBlueprintMap::ProcedureBlueprintMap() {
}

ProcedureBlueprintMap::~ProcedureBlueprintMap() {
}

const ProcedureBlueprint* ProcedureBlueprintMap::getBlueprint(const std::string_view& name) {
    for (const auto& blueprint : _blueprints) {
        if (blueprint._name == name) {
            msgbioassert(blueprint._valid, "Procedure blueprint is not valid");
            return &blueprint;
        }
    }

    return nullptr;
}
