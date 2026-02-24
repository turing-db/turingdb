#pragma once

#include "EntityPattern.h"

namespace db {

class CypherAST;
class Symbol;
class SymbolChain;
class EdgePatternData;
class QuantifiedPath;

class EdgePattern : public EntityPattern {
public:
    enum class Direction {
        Undirected = 0,
        Backward,
        Forward
    };

    static EdgePattern* create(CypherAST* ast, QuantifiedPath* quantified, Direction direction);

    Direction getDirection() const { return _direction; }

    const SymbolChain* types() const { return _types; }

    EdgePatternData* getData() const { return _data; }

    QuantifiedPath* getQuantifiedPath() const { return _quantifiedPath; }

    void setDirection(Direction direction) { _direction = direction; }

    void setTypes(SymbolChain* types) { _types = types; }

    void setData(EdgePatternData* data) { _data = data; }

    void setQuanfiedPath(QuantifiedPath* quantifiedPath) { _quantifiedPath = quantifiedPath; }

private:
    Direction _direction {Direction::Undirected};
    SymbolChain* _types {nullptr};
    EdgePatternData* _data {nullptr};
    QuantifiedPath* _quantifiedPath {nullptr};

    EdgePattern(Direction direction);
    ~EdgePattern() override;
};

}
