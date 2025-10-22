#pragma once

#include <string_view>
#include <map>

#include "decl/EvaluatedType.h"

namespace db::v2 {

class FunctionDecls {
public:
    struct FunctionSignature {
        std::string_view _fullName;
        std::vector<EvaluatedType> _argumentTypes;
        EvaluatedType _returnType {};
        bool _isAggregate {false};
    };

    FunctionDecls();
    ~FunctionDecls();

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    void add(const FunctionSignature& signature);

    auto get(std::string_view fullName) const {
        return _decls.equal_range(fullName);
    }

private:
    std::multimap<std::string_view, FunctionSignature> _decls;
};

}
