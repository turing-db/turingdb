#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;
class EdgePattern;
class PropertyExpr;

class CreateEdgePropertyIndexQuery final : public QueryCommand {
public:
    static CreateEdgePropertyIndexQuery* create(CypherAST* ast,
                                                std::string_view indexName,
                                                EdgePattern* edge,
                                                PropertyExpr* property);

    Kind getKind() const final { return Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY; }

    const EdgePattern* edgePattern() const { return _edgePattern; }
    PropertyExpr* propertyExpr() const { return _propertyExpr; }
    std::string_view indexName() const { return _indexName; }

private:
    CreateEdgePropertyIndexQuery(DeclContext* declCtxt,
                                 std::string_view indexName,
                                 EdgePattern* edge,
                                 PropertyExpr* property);

    ~CreateEdgePropertyIndexQuery() final = default;

    std::string_view _indexName;
    EdgePattern* _edgePattern {nullptr};
    PropertyExpr* _propertyExpr {nullptr};
};

}
