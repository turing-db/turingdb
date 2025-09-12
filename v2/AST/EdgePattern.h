#pragma once

#include <vector>

#include "EntityPattern.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class EdgePatternData;

class EdgePattern : public EntityPattern {
public:
    using EdgeTypes = std::vector<Symbol*>;

    static EdgePattern* create(CypherAST* ast);

    const EdgeTypes& types() const { return _types; }

    EdgePatternData* getData() const { return _data; }

    void setTypes(EdgeTypes&& types) { _types = std::move(types); }

    void setData(EdgePatternData* data) { _data = data; }

private:
    EdgeTypes _types;
    EdgePatternData* _data {nullptr};

    EdgePattern();
    ~EdgePattern() override;
};

}
