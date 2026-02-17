#pragma once

#include <memory>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "FunctionResolver.h"

namespace db {

class FunctionSignature;
class ProcedureManager;

class ProcedureLookup : public FunctionResolver {
public:
    explicit ProcedureLookup(const ProcedureManager* manager);
    ~ProcedureLookup() override;

    ProcedureLookup(const ProcedureLookup&) = delete;
    ProcedureLookup(ProcedureLookup&&) = delete;
    ProcedureLookup& operator=(const ProcedureLookup&) = delete;
    ProcedureLookup& operator=(ProcedureLookup&&) = delete;

    FunctionSignatureRange lookup(std::string_view fullName) override;

private:
    const ProcedureManager* _manager {nullptr};
    std::vector<std::unique_ptr<FunctionSignature>> _cacheOwned;
    std::unordered_map<std::string_view, FunctionSignature*> _cacheMap;
};

}
