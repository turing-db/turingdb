#pragma once

#include <string_view>
#include <memory>
#include <map>

#include "FunctionSignature.h"

namespace db {

class FunctionDecls {
public:
    FunctionDecls();
    ~FunctionDecls();

    FunctionDecls(const FunctionDecls&) = delete;
    FunctionDecls(FunctionDecls&&) = delete;
    FunctionDecls& operator=(const FunctionDecls&) = delete;
    FunctionDecls& operator=(FunctionDecls&&) = delete;

    void initDefault();

    FunctionSignature* createFunction(std::string_view fullName);

    auto get(std::string_view fullName) const {
        return _decls.equal_range(fullName);
    }

private:
    std::multimap<std::string_view, std::unique_ptr<FunctionSignature>> _decls;
};

}
