#include "FunctionNamespace.h"

#include <ctype.h>
#include <algorithm>
#include <utility>

using namespace db;

namespace {

// Cypher matches a function name whatever its case, namespace included: stdev is stDev,
// COUNT is count and COLL.INDEXOF is coll.indexOf
void foldCase(std::string_view name, std::string& folded) {
    folded.resize(name.size());
    std::ranges::transform(name, folded.begin(), [](unsigned char letter) {
        return static_cast<char>(tolower(letter));
    });
}

}

FunctionNamespace::FunctionNamespace(std::string_view fullName)
    : _fullName(fullName)
{
}

FunctionNamespace::~FunctionNamespace() {
}

const FunctionNamespace* FunctionNamespace::getNamespace(std::string_view name) const {
    std::string foldedName;
    foldCase(name, foldedName);

    const auto it = _namespaces.find(foldedName);
    if (it == _namespaces.end()) {
        return nullptr;
    }

    return it->second.get();
}

const FunctionNamespace::FunctionSignatures* FunctionNamespace::getFunction(std::string_view name) const {
    std::string foldedName;
    foldCase(name, foldedName);

    const auto it = _functionMap.find(foldedName);
    if (it == _functionMap.end()) {
        return nullptr;
    }

    return &it->second;
}

FunctionNamespace* FunctionNamespace::createNamespace(std::string_view name) {
    std::string foldedName;
    foldCase(name, foldedName);

    std::string fullName;
    qualify(name, fullName);

    std::unique_ptr<FunctionNamespace>& child = _namespaces[foldedName];
    child = std::make_unique<FunctionNamespace>(fullName);

    return child.get();
}

FunctionSignature* FunctionNamespace::createFunction(std::string_view name) {
    std::string fullName;
    qualify(name, fullName);

    FunctionSignature* function = _functions.emplace_back(std::make_unique<FunctionSignature>(fullName)).get();

    std::string foldedName;
    foldCase(name, foldedName);
    _functionMap[foldedName].push_back(function);

    return function;
}

void FunctionNamespace::addAlias(std::string_view alias, std::string_view name) {
    std::string foldedAlias;
    foldCase(alias, foldedAlias);

    std::string foldedName;
    foldCase(name, foldedName);

    const FunctionSignatures& signatures = _functionMap.at(foldedName);
    _functionMap[foldedAlias] = signatures;
}

void FunctionNamespace::createOverloads(std::string_view name,
                                        const std::vector<PossibleArgTypes>& positionTypes,
                                        std::vector<FunctionSignature*>& overloads) {
    size_t overloadCount = 1;
    for (const PossibleArgTypes& types : positionTypes) {
        overloadCount *= types.size();
    }

    for (size_t overloadIndex = 0; overloadIndex < overloadCount; overloadIndex++) {
        FunctionSignature::ArgumentTypes arguments;

        size_t remaining = overloadIndex;
        for (const PossibleArgTypes& types : positionTypes) {
            arguments.push_back(types[remaining % types.size()]);
            remaining /= types.size();
        }

        FunctionSignature* overload = createFunction(name);
        overload->setArguments(std::move(arguments));
        overloads.push_back(overload);
    }
}

void FunctionNamespace::qualify(std::string_view name, std::string& qualified) const {
    qualified = _fullName;

    if (!qualified.empty()) {
        qualified += '.';
    }

    qualified += name;
}
