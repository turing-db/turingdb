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

    ~FunctionDecls() override;

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    static const FunctionDecls& getBuiltins();

    FunctionSignatureRange lookup(std::string_view fullName) const override;

private:
    std::vector<std::unique_ptr<FunctionSignature>> _owned;
    std::unordered_map<std::string_view, FunctionSignatures> _nameMap;

    FunctionDecls();

    void initDefault();

    FunctionSignature* createFunction(std::string_view fullName);

    // One overload per way of typing the arguments, position i taking any of positionTypes[i]
    void createOverloads(std::string_view fullName,
                         const std::vector<std::vector<FunctionArgumentType>>& positionTypes,
                         std::vector<FunctionSignature*>& overloads);
};

}
