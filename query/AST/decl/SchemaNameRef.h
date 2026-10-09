#pragma once

#include <span>
#include <string_view>
#include <vector>

namespace db {

class SymbolChain;

enum class SchemaNameKind {
    Label,
    EdgeType,
};

// A label or an edge type a pattern asks for: a name the query wrote, or the parameter
// the name is bound from when the program runs
template <SchemaNameKind Kind>
struct SchemaNameRef {
    std::string_view _name;
    bool _isParameter {false};

    bool operator==(const SchemaNameRef& other) const;
};

using LabelRef = SchemaNameRef<SchemaNameKind::Label>;
using EdgeTypeRef = SchemaNameRef<SchemaNameKind::EdgeType>;

template <SchemaNameKind Kind>
void collectSchemaNames(const SymbolChain* chain, std::vector<SchemaNameRef<Kind>>& refs);

template <SchemaNameKind Kind>
bool holdsParameter(std::span<const SchemaNameRef<Kind>> refs);

}
