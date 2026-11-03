#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "FunctionSignature.h"

namespace db {

class FunctionNamespace {
public:
    using FunctionSignatures = std::vector<FunctionSignature*>;
    using PossibleArgTypes = std::vector<FunctionArgumentType>;

    explicit FunctionNamespace(std::string_view fullName);
    ~FunctionNamespace();

    FunctionNamespace(const FunctionNamespace&) = delete;
    FunctionNamespace(FunctionNamespace&&) = delete;
    FunctionNamespace& operator=(const FunctionNamespace&) = delete;
    FunctionNamespace& operator=(FunctionNamespace&&) = delete;

    std::string_view getFullName() const { return _fullName; }

    const FunctionNamespace* getNamespace(std::string_view name) const;

    const FunctionSignatures* getFunction(std::string_view name) const;

    FunctionNamespace* createNamespace(std::string_view name);

    FunctionSignature* createFunction(std::string_view name);

    // @param alias resolves to every signature of @param name, which codegen reads by that name
    void addAlias(std::string_view alias, std::string_view name);

    // One overload per way of typing the arguments, position i taking any of positionTypes[i]
    void createOverloads(std::string_view name,
                         const std::vector<PossibleArgTypes>& positionTypes,
                         std::vector<FunctionSignature*>& overloads);

private:
    std::string _fullName;
    std::unordered_map<std::string, std::unique_ptr<FunctionNamespace>> _namespaces;
    std::vector<std::unique_ptr<FunctionSignature>> _functions;
    std::unordered_map<std::string, FunctionSignatures> _functionMap;

    void qualify(std::string_view name, std::string& qualified) const;
};

}
