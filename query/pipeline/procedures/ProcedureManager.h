#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace db {

class Procedure;
class ProcedureNamespace;

class ProcedureManager {
public:
    using Namespaces = std::vector<ProcedureNamespace*>;

    ~ProcedureManager();

    void init();

    void getNamespaces(Namespaces& result) const;

    const Procedure* getProcedure(std::string_view fullName) const;

    ProcedureNamespace* getNamespace(std::string_view name) const;

    ProcedureNamespace* createNamespace(std::string_view name);

    static std::unique_ptr<ProcedureManager> create();

private:
    mutable std::shared_mutex _mutex;
    Namespaces _namespaces;
    std::unordered_map<std::string_view, ProcedureNamespace*> _namespaceMap;

    ProcedureManager();
};

}
