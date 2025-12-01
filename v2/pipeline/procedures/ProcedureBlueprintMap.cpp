#include "ProcedureBlueprintMap.h"

#include "ScanLabelsProcedure.h"

using namespace db::v2;

const std::array<ProcedureBlueprint, 1> ProcedureBlueprintMap::_blueprints {
    {ScanLabelsProcedure::Blueprint}
};

ProcedureBlueprintMap::ProcedureBlueprintMap() {
}

ProcedureBlueprintMap::~ProcedureBlueprintMap() {
}

const ProcedureBlueprint* ProcedureBlueprintMap::getBlueprint(const std::string_view& name) {
    if (name == "db.labels") {
        return &ScanLabelsProcedure::Blueprint;
    }

    return nullptr;
}
