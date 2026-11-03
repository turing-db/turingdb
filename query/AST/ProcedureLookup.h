#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "FunctionResolver.h"

namespace db {

class FunctionSignature;
class Procedure;
class ProcedureManager;

class ProcedureLookup : public FunctionResolver {
public:
    explicit ProcedureLookup(const ProcedureManager* manager);
    ~ProcedureLookup() override;

    ProcedureLookup(const ProcedureLookup&) = delete;
    ProcedureLookup(ProcedureLookup&&) = delete;
    ProcedureLookup& operator=(const ProcedureLookup&) = delete;
    ProcedureLookup& operator=(ProcedureLookup&&) = delete;

    FunctionSignatureRange lookup(const QualifiedName* name) const override;

private:
    const ProcedureManager* _manager {nullptr};
    mutable std::vector<std::unique_ptr<FunctionSignature>> _cacheOwned;
    mutable std::unordered_map<const Procedure*, FunctionSignature*> _cacheMap;
};

}
