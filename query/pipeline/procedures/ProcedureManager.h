#pragma once

#include <memory>
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

    void init();

    const Namespaces& namespaces() const { return _namespaces; }

    const Procedure* getProcedure(std::string_view fullName) const;

    ProcedureNamespace* getNamespace(std::string_view name) const;

    ProcedureNamespace* createNamespace(std::string_view name);

    static std::unique_ptr<ProcedureManager> create();

private:
    Namespaces _namespaces;
    std::unordered_map<std::string_view, ProcedureNamespace*> _namespaceMap;
};

}
