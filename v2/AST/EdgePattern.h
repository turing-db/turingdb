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

    enum class Direction {
        Undirected = 0,
        Backward,
        Forward
    };

    static EdgePattern* create(CypherAST* ast, Direction direction);

    Direction getDirection() const { return _direction; }

    const EdgeTypes& types() const { return _types; }

    EdgePatternData* getData() const { return _data; }

    void setDirection(Direction direction) { _direction = direction; }

    void setTypes(EdgeTypes&& types) { _types = std::move(types); }

    void setData(EdgePatternData* data) { _data = data; }

private:
    Direction _direction {Direction::Undirected};
    EdgeTypes _types;
    EdgePatternData* _data {nullptr};

    EdgePattern(Direction direction);
    ~EdgePattern() override;
};

}
