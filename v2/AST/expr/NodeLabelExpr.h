#pragma once

#include "Expr.h"

#include <vector>

#include "metadata/LabelSet.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class VarDecl;

class NodeLabelExpr : public Expr {
public:
    using Labels = std::vector<Symbol*>;

    Symbol* getSymbol() const { return _symbol; }
    const Labels& labels() const { return _labels; }

    static NodeLabelExpr* create(CypherAST* ast, 
                                 Symbol* symbol,
                                 Labels&& labels);

    VarDecl* getDecl() const { return _decl; }
    const LabelSet& labelSet() const { return _labelSet; }

    void setDecl(VarDecl* decl) { _decl = decl; }
    void setLabelID(LabelID labelID) { _labelSet.set(labelID); }

private:
    Symbol* _symbol {nullptr};
    Labels _labels;
    VarDecl* _decl {nullptr};
    LabelSet _labelSet;

    NodeLabelExpr(Symbol* symbol, Labels&& labels);
    ~NodeLabelExpr() override;
};

}
