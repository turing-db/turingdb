#pragma once

#include <string_view>
#include <unordered_map>

#include "decl/EvaluatedType.h"

namespace db::v2 {

class FunctionDecls {
public:
    struct FunctionSignature {
        std::string_view _fullName;
        std::vector<EvaluatedType> _argumentTypes;
        EvaluatedType _returnType {};
        bool _isAggregate {};
    };

    FunctionDecls();
    ~FunctionDecls();

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    void add(const FunctionSignature& signature);

    const FunctionSignature* get(std::string_view name) const;

private:
    std::unordered_map<std::string_view, FunctionSignature> _decls;
};

}
