#include "ProcedureManager.h"

#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "LabelsProcedure.h"
#include "EdgeTypesProcedure.h"
#include "PropertyTypesProcedure.h"
#include "HistoryProcedure.h"
#include "DescribeCommitProcedure.h"
#include "ProceduresProcedure.h"
#include "ShowIndexesProcedure.h"
#include "NodesProcedure.h"
#include "EdgesProcedure.h"
#include "NeighborsProcedure.h"
#include "NodeEdgesProcedure.h"
#include "ExploreNodeEdgesProcedure.h"
#include "NodePropertiesProcedure.h"

using namespace db;

ProcedureManager::ProcedureManager()
{
}

ProcedureManager::~ProcedureManager() {
    for (auto* ns : _namespaces) {
        delete ns;
    }
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
    ShowIndexesProcedure::registerProcedure(db);
    NodesProcedure::registerProcedure(db);
    EdgesProcedure::registerProcedure(db);
    NeighborsProcedure::registerProcedure(db);
    NodeEdgesProcedure::registerProcedure(db);
    ExploreNodeEdgesProcedure::registerProcedure(db);
    NodePropertiesProcedure::registerProcedure(db);
}

void ProcedureManager::getNamespaces(Namespaces& result) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    result = _namespaces;
}

const Procedure* ProcedureManager::getProcedure(std::string_view fullName) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    const size_t dot = fullName.find('.');
    if (dot == std::string_view::npos) {
        return nullptr;
    }

    const std::string_view nsName = fullName.substr(0, dot);
    const std::string_view procName = fullName.substr(dot + 1);

    const auto it = _namespaceMap.find(nsName);
    if (it == _namespaceMap.end()) {
        return nullptr;
    }

    return it->second->getProcedure(procName);
}

ProcedureNamespace* ProcedureManager::getNamespace(std::string_view name) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    const auto it = _namespaceMap.find(name);
    if (it == _namespaceMap.end()) {
        return nullptr;
    }

    return it->second;
}

ProcedureNamespace* ProcedureManager::createNamespace(std::string_view name) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    ProcedureNamespace* ns = new ProcedureNamespace(name);
    _namespaces.push_back(ns);
    _namespaceMap[name] = ns;
    return ns;
}
