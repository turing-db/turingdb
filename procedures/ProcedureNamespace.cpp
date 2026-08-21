#include "ProcedureNamespace.h"

#include <memory>

#include "Procedure.h"
#include "ProcedureException.h"

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

    // The namespace owns the declaration from here on, so one it goes on to refuse is
    // freed rather than left behind by a caller that registers and moves on.
    std::unique_ptr<Procedure> owned(procedure);

    std::string& fullName = procedure->_fullName;
    fullName.clear();
    fullName += _name;
    fullName += ".";
    fullName += procedure->getName();

    // Nothing can be carried past a call whose procedure cannot say which of the rows it
    // was driven with produced each row it emits, so the declaration is refused up front
    // rather than at the first query that needs the report.
    if (procedure->hasRowAlignedArgument() && !procedure->hasIndices()) {
        throw ProcedureException("Procedure '" + fullName + "' takes an argument aligned with the "
                                 "rows it is driven with, so it must report the input row of the "
                                 "rows it emits");
    }

    _procedures.push_back(owned.release());
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
