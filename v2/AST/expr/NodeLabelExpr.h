#pragma once

#include <vector>

#include "Expr.h"
#include "metadata/LabelSet.h"
#include "Symbol.h"

namespace db::v2 {

class VarDecl;
class CypherAST;

class NodeLabelExpr : public Expr {
public:
    using LabelVector = std::vector<std::string_view>;

    bool hasDecl() const { return _decl != nullptr; }

    const VarDecl& decl() const { return *_decl; }
    const Symbol& symbol() const { return _symbol; }
    const LabelVector& labelNames() const { return _labelVector; }
    const LabelSet& labels() const { return _labelset; }
    LabelSet& labels() { return _labelset; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

    static NodeLabelExpr* create(CypherAST* ast, 
                                 const Symbol& symbol,
                                 LabelVector&& labels);

private:
    Symbol _symbol;
    LabelVector _labelVector;
    LabelSet _labelset;
    const VarDecl* _decl {nullptr};

    NodeLabelExpr(const Symbol& symbol, LabelVector&& labels)
        : Expr(Kind::NODE_LABEL),
        _symbol(symbol),
        _labelVector(std::move(labels))
    {
    }

    ~NodeLabelExpr() override;
};

}
