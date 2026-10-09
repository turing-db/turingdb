#include "SchemaNameRef.h"

#include <algorithm>

#include "Parameter.h"
#include "Symbol.h"
#include "SymbolChain.h"

using namespace db;

template <SchemaNameKind Kind>
bool SchemaNameRef<Kind>::operator==(const SchemaNameRef& other) const {
    return _name == other._name && _isParameter == other._isParameter;
}

template <SchemaNameKind Kind>
void db::collectSchemaNames(const SymbolChain* chain, std::vector<SchemaNameRef<Kind>>& refs) {
    for (const Symbol* symbol : *chain) {
        refs.emplace_back(symbol->getName(), false);
    }

    for (const Parameter* parameter : chain->getParameters()) {
        refs.emplace_back(parameter->getName(), true);
    }
}

template <SchemaNameKind Kind>
bool db::holdsParameter(std::span<const SchemaNameRef<Kind>> refs) {
    return std::ranges::any_of(refs, [](const SchemaNameRef<Kind>& ref) { return ref._isParameter; });
}

template struct db::SchemaNameRef<SchemaNameKind::Label>;
template struct db::SchemaNameRef<SchemaNameKind::EdgeType>;
template void db::collectSchemaNames(const SymbolChain* chain, std::vector<LabelRef>& refs);
template void db::collectSchemaNames(const SymbolChain* chain, std::vector<EdgeTypeRef>& refs);
template bool db::holdsParameter(std::span<const LabelRef> refs);
template bool db::holdsParameter(std::span<const EdgeTypeRef> refs);
