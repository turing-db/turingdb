#include "Procedure.h"

#include "ProcedureException.h"

#include "BioAssert.h"

using namespace db;

Procedure::Procedure(std::string_view name)
    : _name(name)
{
}

Procedure::~Procedure() {
}

void Procedure::setExecuteCallback(ExecuteCallback cb) {
    _execCallback = cb;
}

void Procedure::setAllocCallback(AllocCallback cb) {
    _allocCallback = cb;
}

void Procedure::setDeallocCallback(DeallocCallback cb) {
    _deallocCallback = cb;
}

void Procedure::addReturnValue(std::string_view name, ProcedureType type) {
    _returnValues.add(name, type);
}

void Procedure::addArgument(std::string_view name, ProcedureType type) {
    throwIfOptionalArgumentDeclared();
    _argumentTypes.add(name, type);
}

void Procedure::addOptionalArgument(std::string_view name, ProcedureType type) {
    _argumentTypes.addOptional(name, type);
}

void Procedure::addConstantArgument(std::string_view name, ProcedureType type) {
    throwIfOptionalArgumentDeclared();
    _argumentTypes.addConstant(name, type);
}

void Procedure::addOptionalConstantArgument(std::string_view name, ProcedureType type) {
    _argumentTypes.addOptionalConstant(name, type);
}

size_t Procedure::getRequiredArgumentCount() const {
    return _argumentTypes.requiredCount();
}

size_t Procedure::getReturnValueIndex(std::string_view name) const {
    for (size_t i = 0; i < _returnValues.size(); ++i) {
        if (_returnValues[i]._name == name) {
            return i;
        }
    }

    throw ProcedureException("Procedure '" + _fullName + "' does not return a value named '"
                             + std::string(name) + "'");
}

size_t Procedure::getArgumentIndex(std::string_view name) const {
    for (size_t i = 0; i < _argumentTypes.size(); ++i) {
        if (_argumentTypes[i]._name == name) {
            return i;
        }
    }

    throw ProcedureException("Column is not an argument of the procedure");
}

void Procedure::returnAll(std::vector<YieldItem>& yieldItems) const {
    for (const auto& item : _returnValues) {
        bioassert(item._type != ProcedureType::INVALID,
                  "Invalid procedure return type");

        yieldItems.emplace_back(item._name);
    }
}

bool Procedure::hasRowAlignedArgument() const {
    for (const NamedProcedureType& argument : _argumentTypes) {
        if (!argument._constant) {
            return true;
        }
    }

    return false;
}

void Procedure::throwIfOptionalArgumentDeclared() const {
    if (_argumentTypes.requiredCount() < _argumentTypes.size()) {
        throw ProcedureException("Required arguments must precede optional arguments");
    }
}
