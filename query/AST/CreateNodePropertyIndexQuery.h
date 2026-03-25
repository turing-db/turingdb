#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;
class NodePattern;
class PropertyExpr;

class CreateNodePropertyIndexQuery final : public QueryCommand {
public:
    static CreateNodePropertyIndexQuery* create(CypherAST* ast,
                                                std::string_view indexName,
                                                NodePattern* node,
                                                PropertyExpr* property);

    Kind getKind() const final { return Kind::CREATE_NODE_PROPERTY_INDEX_QUERY; }

    const NodePattern* nodePattern() const { return _nodePattern; }
    PropertyExpr* propertyExpr() const { return _propertyExpr; }
    std::string_view indexName() const { return _indexName; }

private:
    CreateNodePropertyIndexQuery(DeclContext* declCtxt,
                                 std::string_view indexName,
                                 NodePattern* node,
                                 PropertyExpr* property);

    ~CreateNodePropertyIndexQuery() final = default;

    std::string_view _indexName;
    NodePattern* _nodePattern {nullptr};
    PropertyExpr* _propertyExpr {nullptr};
};

}
