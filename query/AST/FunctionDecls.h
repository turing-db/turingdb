#pragma once

#include <string_view>
#include <memory>
#include <unordered_map>
#include <vector>

#include "FunctionResolver.h"
#include "FunctionSignature.h"

namespace db {

class FunctionDecls : public FunctionResolver {
public:
    using FunctionSignatures = std::vector<FunctionSignature*>;

    FunctionDecls();
    ~FunctionDecls() override;

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    void initDefault();

    FunctionSignature* createFunction(std::string_view fullName);

    FunctionSignatureRange lookup(std::string_view fullName) override;

private:
    std::vector<std::unique_ptr<FunctionSignature>> _owned;
    std::unordered_map<std::string_view, FunctionSignatures> _nameMap;
};

}
