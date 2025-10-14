#include "FunctionDecls.h"

using namespace db::v2;

FunctionDecls::FunctionDecls() {
}

FunctionDecls::~FunctionDecls() {
}

void FunctionDecls::add(const FunctionSignature& signature) {
    _decls.emplace(signature._fullName, signature);
}

const FunctionDecls::FunctionSignature* FunctionDecls::get(std::string_view name) const {
    const auto it = _decls.find(name);
    if (it == _decls.end()) {
        return nullptr;
    }

    return &it->second;
}

