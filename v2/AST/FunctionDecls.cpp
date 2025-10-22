#include "FunctionDecls.h"

using namespace db::v2;

FunctionDecls::FunctionDecls()
{
}

FunctionDecls::~FunctionDecls() {
}

void FunctionDecls::add(const FunctionSignature& signature) {
    _decls.emplace(signature._fullName, signature);
}
