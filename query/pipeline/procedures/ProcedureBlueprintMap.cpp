#include "ProcedureBlueprintMap.h"

#include "LabelsProcedure.h"
#include "EdgeTypesProcedure.h"
#include "PropertyTypesProcedure.h"
#include "HistoryProcedure.h"
#include "ProceduresProcedure.h"

using namespace db;

ProcedureBlueprintMap::ProcedureBlueprintMap()
{
}

ProcedureBlueprintMap::~ProcedureBlueprintMap() {
}

std::unique_ptr<ProcedureBlueprintMap> ProcedureBlueprintMap::create() {
    auto map = std::make_unique<ProcedureBlueprintMap>();
    map->_blueprints.emplace_back(LabelsProcedure::createBlueprint());
    map->_blueprints.emplace_back(PropertyTypesProcedure::createBlueprint());
    map->_blueprints.emplace_back(EdgeTypesProcedure::createBlueprint());
    map->_blueprints.emplace_back(HistoryProcedure::createBlueprint());
    map->_blueprints.emplace_back(ProceduresProcedure::createBlueprint());

    return map;
}

const ProcedureBlueprint* ProcedureBlueprintMap::getBlueprint(const std::string_view& name) const {
    for (const auto& blueprint : _blueprints) {
        if (blueprint._name == name) {
            return &blueprint;
        }
    }

    return nullptr;
}
