#pragma once

#include <string_view>
#include <vector>

#include "FunctionNamespace.h"
#include "FunctionResolver.h"
#include "FunctionSignature.h"

namespace db {

class FunctionDecls : public FunctionResolver {
public:
    using PossibleArgTypes = FunctionNamespace::PossibleArgTypes;

    ~FunctionDecls() override;

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    static const FunctionDecls& getBuiltins();

    FunctionSignatureRange lookup(const QualifiedName* name) const override;

private:
    FunctionNamespace _root {""};

    FunctionDecls();

    void initDefault();

    void initCollectionFunctions();

    FunctionSignature* createFunction(std::string_view name);

    void addAlias(std::string_view alias, std::string_view name);

    void createOverloads(std::string_view name,
                         const std::vector<PossibleArgTypes>& positionTypes,
                         std::vector<FunctionSignature*>& overloads);
};

}
