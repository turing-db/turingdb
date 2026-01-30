#pragma once

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

    ProcedureNamespace(const ProcedureNamespace&) = delete;
    ProcedureNamespace& operator=(const ProcedureNamespace&) = delete;
    ProcedureNamespace(ProcedureNamespace&&) = delete;
    ProcedureNamespace& operator=(ProcedureNamespace&&) = delete;

    void addProcedure(Procedure* procedure);
    const Procedure* getProcedure(std::string_view name) const;

    std::string_view getName() const { return _name; }
    const Procedures& procedures() const { return _procedures; }

private:
    std::string_view _name;
    Procedures _procedures;
    std::unordered_map<std::string_view, Procedure*> _procedureMap;
};

}
