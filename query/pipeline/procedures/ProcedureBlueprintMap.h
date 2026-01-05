#pragma once

#include <string_view>
#include <vector>
#include <memory>

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

    static std::unique_ptr<ProcedureBlueprintMap> create();

    const ProcedureBlueprint* getBlueprint(const std::string_view& name) const;
    const std::vector<ProcedureBlueprint>& getAll() const { return _blueprints; }

private:
    std::vector<ProcedureBlueprint> _blueprints;
};

}
