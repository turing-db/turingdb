#pragma once

#include <string_view>
#include <array>

#include "ProcedureBlueprint.h"

namespace db::v2 {

class ProcedureBlueprintMap {
public:
    ProcedureBlueprintMap();
    ~ProcedureBlueprintMap();

    ProcedureBlueprintMap(const ProcedureBlueprintMap&) = delete;
    ProcedureBlueprintMap& operator=(const ProcedureBlueprintMap&) = delete;
    ProcedureBlueprintMap(ProcedureBlueprintMap&&) = delete;
    ProcedureBlueprintMap& operator=(ProcedureBlueprintMap&&) = delete;

    static const ProcedureBlueprint* getBlueprint(const std::string_view& name);

private:
    static const std::array<ProcedureBlueprint, 1> _blueprints;
};

}
