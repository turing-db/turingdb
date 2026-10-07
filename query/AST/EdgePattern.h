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

    // The hops a quantified pattern repeats: (n)((a)-[e]->(b)-[f]->(c) WHERE ...){1,3}(m)
    // repeats e then f. A pattern of one hop is that hop's own edge pattern, and one of
    // several an edge pattern of its own holding them.
    size_t getHopCount() const { return _hops.empty() ? 1 : _hops.size(); }
    EdgePattern* getHop(size_t hop) { return _hops.empty() ? this : _hops[hop]; }
    const EdgePattern* getHop(size_t hop) const { return _hops.empty() ? this : _hops[hop]; }

    // The nodes of the parenthesized form, one more than its hops, and the predicate, which
    // constrain every repetition of the pattern. Null for the bracketed form, [e*1..3].
    NodePattern* getHopNode(size_t position) const { return _hopNodes.empty() ? nullptr : _hopNodes[position]; }
    WhereClause* getHopWhere() const { return _hopWhere; }

    // The declarations of a quantified pattern: the single edge a hop's variable is inside
    // the pattern, and the group variable each named node is outside it
    VarDecl* getHopDecl() const { return _hopDecl; }
    VarDecl* getHopNodeGroup(size_t position) const { return _hopNodeGroups.empty() ? nullptr : _hopNodeGroups[position]; }

    // Every analyzed predicate a hop of a quantified pattern must pass
    const std::vector<Expr*>& hopPredicates() const { return _hopPredicates; }

    // The variables a hop predicate reads from outside the hop: one value each for the whole
    // walk leaving a seed, which the exploration hands the predicate alongside the hop
    const std::vector<const VarDecl*>& hopImports() const { return _hopImports; }

    void setDirection(Direction direction) { _direction = direction; }

    void setTypes(SymbolChain* types) { _types = types; }

    void setData(EdgePatternData* data) { _data = data; }

    void setQuantifiedPath(QuantifiedPath* quantifiedPath) { _quantifiedPath = quantifiedPath; }

    void addHop(EdgePattern* hop) { _hops.push_back(hop); }
    void addHopNode(NodePattern* node) { _hopNodes.push_back(node); }
    void setHopWhere(WhereClause* where) { _hopWhere = where; }
    void setHopDecl(VarDecl* decl) { _hopDecl = decl; }
    void addHopNodeGroup(VarDecl* decl) { _hopNodeGroups.push_back(decl); }
    void addHopPredicate(Expr* predicate) { _hopPredicates.push_back(predicate); }
    void addHopImport(const VarDecl* decl) { _hopImports.push_back(decl); }

private:
    Direction _direction {Direction::Undirected};
    SymbolChain* _types {nullptr};
    EdgePatternData* _data {nullptr};
    QuantifiedPath* _quantifiedPath {nullptr};
    std::vector<EdgePattern*> _hops;
    std::vector<NodePattern*> _hopNodes;
    WhereClause* _hopWhere {nullptr};
    VarDecl* _hopDecl {nullptr};
    std::vector<VarDecl*> _hopNodeGroups;
    std::vector<Expr*> _hopPredicates;
    std::vector<const VarDecl*> _hopImports;

    EdgePattern(Direction direction);
    ~EdgePattern() override;
};

}
