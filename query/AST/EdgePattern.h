#pragma once

#include <vector>

#include "EntityPattern.h"

namespace db {

class CypherAST;
class Expr;
class NodePattern;
class Symbol;
class SymbolChain;
class EdgePatternData;
class QuantifiedPath;
class VarDecl;
class WhereClause;

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

    // The parenthesized form of a quantified pattern, (n)((a)-[e]->(b) WHERE ...){1,3}(m):
    // the two inner nodes and the predicate, which constrain every hop of the path
    NodePattern* getHopSource() const { return _hopSource; }
    NodePattern* getHopEnd() const { return _hopEnd; }
    WhereClause* getHopWhere() const { return _hopWhere; }

    // The declarations of one hop of a quantified pattern: the single edge the variable is
    // inside the pattern, and the group variables the inner nodes are outside it
    VarDecl* getHopDecl() const { return _hopDecl; }
    VarDecl* getHopSourceGroup() const { return _hopSourceGroup; }
    VarDecl* getHopEndGroup() const { return _hopEndGroup; }

    // Every analyzed predicate a hop of a quantified pattern must pass
    const std::vector<Expr*>& hopPredicates() const { return _hopPredicates; }

    // The variables a hop predicate reads from outside the hop: one value each for the whole
    // walk leaving a seed, which the exploration hands the predicate alongside the hop
    const std::vector<const VarDecl*>& hopImports() const { return _hopImports; }

    void setDirection(Direction direction) { _direction = direction; }

    void setTypes(SymbolChain* types) { _types = types; }

    void setData(EdgePatternData* data) { _data = data; }

    void setQuantifiedPath(QuantifiedPath* quantifiedPath) { _quantifiedPath = quantifiedPath; }

    void setHopSource(NodePattern* source) { _hopSource = source; }
    void setHopEnd(NodePattern* end) { _hopEnd = end; }
    void setHopWhere(WhereClause* where) { _hopWhere = where; }
    void setHopDecl(VarDecl* decl) { _hopDecl = decl; }
    void setHopSourceGroup(VarDecl* decl) { _hopSourceGroup = decl; }
    void setHopEndGroup(VarDecl* decl) { _hopEndGroup = decl; }
    void addHopPredicate(Expr* predicate) { _hopPredicates.push_back(predicate); }
    void addHopImport(const VarDecl* decl) { _hopImports.push_back(decl); }

private:
    Direction _direction {Direction::Undirected};
    SymbolChain* _types {nullptr};
    EdgePatternData* _data {nullptr};
    QuantifiedPath* _quantifiedPath {nullptr};
    NodePattern* _hopSource {nullptr};
    NodePattern* _hopEnd {nullptr};
    WhereClause* _hopWhere {nullptr};
    VarDecl* _hopDecl {nullptr};
    VarDecl* _hopSourceGroup {nullptr};
    VarDecl* _hopEndGroup {nullptr};
    std::vector<Expr*> _hopPredicates;
    std::vector<const VarDecl*> _hopImports;

    EdgePattern(Direction direction);
    ~EdgePattern() override;
};

}
