#include "ProcedureManager.h"

#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "LabelsProcedure.h"
#include "EdgeTypesProcedure.h"
#include "PropertyTypesProcedure.h"
#include "HistoryProcedure.h"
#include "DescribeCommitProcedure.h"
#include "ProceduresProcedure.h"

using namespace db;

ProcedureManager::ProcedureManager()
{
}

ProcedureManager::~ProcedureManager() {
    for (auto* ns : _namespaces) {
        delete ns;
    }
}

std::unique_ptr<ProcedureManager> ProcedureManager::create() {
    return std::make_unique<ProcedureManager>();
}

void ProcedureManager::init() {
    // Namespace db
    ProcedureNamespace* db = createNamespace("db");

    LabelsProcedure::registerProcedure(db);
    PropertyTypesProcedure::registerProcedure(db);
    EdgeTypesProcedure::registerProcedure(db);
    HistoryProcedure::registerProcedure(db);
    DescribeCommitProcedure::registerProcedure(db);
    ProceduresProcedure::registerProcedure(db);
}

const Procedure* ProcedureManager::getProcedure(std::string_view fullName) const {
    const size_t dot = fullName.find('.');
    if (dot == std::string_view::npos) {
        return nullptr;
    }

    const std::string_view nsName = fullName.substr(0, dot);
    const std::string_view procName = fullName.substr(dot + 1);

    const ProcedureNamespace* ns = getNamespace(nsName);
    if (!ns) {
        return nullptr;
    }

    return ns->getProcedure(procName);
}

ProcedureNamespace* ProcedureManager::getNamespace(std::string_view name) const {
    const auto it = _namespaceMap.find(name);
    if (it == _namespaceMap.end()) {
        return nullptr;
    }

    return it->second;
}

ProcedureNamespace* ProcedureManager::createNamespace(std::string_view name) {
    ProcedureNamespace* ns = new ProcedureNamespace(name);
    _namespaces.push_back(ns);
    _namespaceMap[name] = ns;
    return ns;
}
