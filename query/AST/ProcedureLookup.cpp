#include "ProcedureLookup.h"

#include "FunctionSignature.h"

#include "procedures/ProcedureManager.h"
#include "procedures/Procedure.h"

using namespace db;

static EvaluatedType toEvaluatedType(ProcedureType type) {
    switch (type) {
        case ProcedureType::NODE:
            return EvaluatedType::NodePattern;
        break;

        case ProcedureType::EDGE:
            return EvaluatedType::EdgePattern;
        break;

        case ProcedureType::LABEL_ID:
        case ProcedureType::EDGE_TYPE_ID:
        case ProcedureType::PROPERTY_TYPE_ID:
        case ProcedureType::UINT_64:
        case ProcedureType::INT64:
            return EvaluatedType::Integer;
        break;

        case ProcedureType::DOUBLE:
            return EvaluatedType::Double;
        break;

        case ProcedureType::BOOL:
            return EvaluatedType::Bool;
        break;

        case ProcedureType::VALUE_TYPE:
            return EvaluatedType::ValueType;
        break;

        case ProcedureType::STRING_VIEW:
        case ProcedureType::STRING:
            return EvaluatedType::String;
        break;

        case ProcedureType::INVALID:
        case ProcedureType::_SIZE:
            return EvaluatedType::Invalid;
        break;
    }

    return EvaluatedType::Invalid;
}

ProcedureLookup::ProcedureLookup(const ProcedureManager* manager)
    : _manager(manager)
{
}

ProcedureLookup::~ProcedureLookup() {
}

FunctionResolver::FunctionSignatureRange ProcedureLookup::lookup(std::string_view fullName) {
    const auto it = _cacheMap.find(fullName);
    if (it != _cacheMap.end()) {
        return FunctionSignatureRange(&it->second, &it->second + 1);
    }

    const Procedure* proc = _manager->getProcedure(fullName);
    if (!proc) {
        return FunctionSignatureRange();
    }

    // Use the Procedure's stable std::string as the backing
    // memory for both the FunctionSignature name and cache key.
    const std::string_view stableName = proc->getFullName();

    auto sig = std::make_unique<FunctionSignature>(stableName);
    sig->setIsProcedure(true);

    // Convert argument types
    const ProcedureTypeVector& args = proc->argumentTypes();
    std::vector<EvaluatedType> argTypes;
    argTypes.reserve(args.size());
    for (const auto& arg : args) {
        argTypes.push_back(toEvaluatedType(arg._type));
    }
    sig->setArguments(std::move(argTypes));

    // Convert return types
    const ProcedureTypeVector& rets = proc->returnValues();
    std::vector<FunctionReturnType> retTypes;
    retTypes.reserve(rets.size());
    for (const auto& ret : rets) {
        FunctionReturnType retType = toEvaluatedType(ret._type);
        retType.setName(ret._name);
        retTypes.push_back(retType);
    }
    sig->setReturnTypes(std::move(retTypes));

    FunctionSignature* ptr = sig.get();
    _cacheOwned.push_back(std::move(sig));

    const auto [inserted, ok] = _cacheMap.emplace(stableName, ptr);
    return FunctionSignatureRange(&inserted->second, &inserted->second + 1);
}
