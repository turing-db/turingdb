#pragma once

#include "views/GraphView.h"

namespace db {

class PlanGraph;
class LocalMemory;
class CypherAST;
class IndexLookupNode;
class PropertyExpr;
class LiteralExpr;
template <typename T>
class ColumnVector;

class PlanOptimizer {
public:
    explicit PlanOptimizer(PlanGraph* plan, GraphView view, LocalMemory* mem, CypherAST* ast);
    ~PlanOptimizer();

    void optimize();

private:
    PlanGraph* _plan {nullptr};
    GraphView _view;
    LocalMemory* _mem {nullptr};
    CypherAST* _ast {nullptr};

    void rewriteScanByLabels();
    void rewriteScanByConstIDs();
    void rewriteConstWriteSources();
    void rewritePropertyFilterWithIndex();
    void rewriteNodePropertyFilterWithIndex();

    /// Attempts to add a ConstScan -> IndexLookup with query contained in @param litExprs
    IndexLookupNode* addIndexLookup(const PropertyExpr* propExpr,
                                    const std::vector<const LiteralExpr*>& litExprs);
};
}
