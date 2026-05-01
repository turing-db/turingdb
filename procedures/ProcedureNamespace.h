#pragma once

#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace db {

class Procedure;

class ProcedureNamespace {
public:
    using Procedures = std::vector<Procedure*>;

    ProcedureNamespace(std::string_view name);
    ~ProcedureNamespace();

    std::string_view getName() const { return _name; }

    const Procedure* getProcedure(std::string_view name) const;

    void getProcedures(Procedures& result) const;

    void addProcedure(Procedure* procedure);

private:
    mutable std::shared_mutex _mutex;
    std::string_view _name;
    Procedures _procedures;
    std::unordered_map<std::string_view, Procedure*> _procedureMap;
};

}
