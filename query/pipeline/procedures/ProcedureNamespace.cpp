#include "ProcedureNamespace.h"

#include "Procedure.h"

using namespace db;

ProcedureNamespace::ProcedureNamespace(std::string_view name)
    : _name(name)
{
}

ProcedureNamespace::~ProcedureNamespace() {
    for (auto* proc : _procedures) {
        delete proc;
    }
}

void ProcedureNamespace::addProcedure(Procedure* procedure) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    std::string& fullName = procedure->_fullName;
    fullName.clear();
    fullName += _name;
    fullName += ".";
    fullName += procedure->getName();

    _procedures.push_back(procedure);
    _procedureMap[procedure->getName()] = procedure;
}

void ProcedureNamespace::getProcedures(Procedures& result) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    result = _procedures;
}

const Procedure* ProcedureNamespace::getProcedure(std::string_view name) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    const auto it = _procedureMap.find(name);
    if (it == _procedureMap.end()) {
        return nullptr;
    }

    return it->second;
}
