#pragma once

#include <string_view>
#include <array>

namespace db::v2 {

class ProcedureBlueprint;

class ProcedureBlueprintMap {
public:
    ProcedureBlueprintMap();
    ~ProcedureBlueprintMap();

    ProcedureBlueprintMap(const ProcedureBlueprintMap&) = delete;
    ProcedureBlueprintMap& operator=(const ProcedureBlueprintMap&) = delete;
    ProcedureBlueprintMap(ProcedureBlueprintMap&&) = delete;
    ProcedureBlueprintMap& operator=(ProcedureBlueprintMap&&) = delete;

    static const ProcedureBlueprint* getBlueprint(const std::string_view& name);
    static const std::array<ProcedureBlueprint, 3>& getAll() { return _blueprints; }

private:
    static const std::array<ProcedureBlueprint, 3> _blueprints;
};

}
