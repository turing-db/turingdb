#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>

namespace db {

class Procedure;
class ProcedureNamespace;

class ProcedureManager {
public:
    using Namespaces = std::vector<ProcedureNamespace*>;

    ProcedureManager();
    ~ProcedureManager();

    ProcedureManager(const ProcedureManager&) = delete;
    ProcedureManager& operator=(const ProcedureManager&) = delete;
    ProcedureManager(ProcedureManager&&) = delete;
    ProcedureManager& operator=(ProcedureManager&&) = delete;

    void init();

    const Procedure* getProcedure(std::string_view fullName) const;
    ProcedureNamespace* getNamespace(std::string_view name) const;
    ProcedureNamespace* createNamespace(std::string_view name);

    const Namespaces& namespaces() const { return _namespaces; }

private:
    Namespaces _namespaces;
    std::unordered_map<std::string_view, ProcedureNamespace*>
        _namespaceMap;
};

}
