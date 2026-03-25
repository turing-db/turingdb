#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class CreatePropertyIndexQuery final : public QueryCommand {
public:
    static CreatePropertyIndexQuery* create(CypherAST* ast);

    Kind getKind() const final { return Kind::CREATE_PROPERTY_INDEX_QUERY; }

    std::string_view getPropertyName() const { return _propName; };

private:
    explicit CreatePropertyIndexQuery(DeclContext* declCtxt,
                                      std::string_view propertyName);
    ~CreatePropertyIndexQuery() final = default;

    std::string_view _propName;
};

}
