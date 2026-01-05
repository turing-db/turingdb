#pragma once

#include "EntityPattern.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class SymbolChain;
class EdgePatternData;

class EdgePattern : public EntityPattern {
public:
    enum class Direction {
        Undirected = 0,
        Backward,
        Forward
    };

    static EdgePattern* create(CypherAST* ast, Direction direction);

    Direction getDirection() const { return _direction; }

    const SymbolChain* types() const { return _types; }

    EdgePatternData* getData() const { return _data; }

    void setDirection(Direction direction) { _direction = direction; }

    void setTypes(SymbolChain* types) { _types = types; }

    void setData(EdgePatternData* data) { _data = data; }

private:
    Direction _direction {Direction::Undirected};
    SymbolChain* _types {nullptr};
    EdgePatternData* _data {nullptr};

    EdgePattern(Direction direction);
    ~EdgePattern() override;
};

}
