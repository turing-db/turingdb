#include "FunctionDecls.h"

using namespace db::v2;

FunctionDecls::FunctionDecls()
{
}

FunctionDecls::~FunctionDecls() {
}

void FunctionDecls::add(const FunctionSignature& signature) {
    // TODO: Find a better way than to make a copy. Overal, the API is bad
    _decls.emplace(signature._fullName, std::make_unique<FunctionSignature>(signature));
}
