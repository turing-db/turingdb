#pragma once

#include <vector>

namespace db {

class CypherAST;
class SymbolChain;

// The labels a node pattern or a label test asks for, combined with & (and), | (or) and
// ! (not). A conjunction of names is one LABELS leaf: the set of labels a node carries.
class LabelExpr {
public:
    friend CypherAST;

    enum class Kind {
        LABELS,
        NOT,
        AND,
        OR,
    };

    using Operands = std::vector<LabelExpr*>;

    static LabelExpr* createLabels(CypherAST* ast, SymbolChain* labels);
    static LabelExpr* createNot(CypherAST* ast, LabelExpr* operand);
    static LabelExpr* createAnd(CypherAST* ast, LabelExpr* lhs, LabelExpr* rhs);
    static LabelExpr* createOr(CypherAST* ast, LabelExpr* lhs, LabelExpr* rhs);

    Kind getKind() const { return _kind; }

    SymbolChain* getLabels() const { return _labels; }

    const Operands& getOperands() const { return _operands; }

    bool holdsParameter() const;

private:
    Kind _kind {Kind::LABELS};
    SymbolChain* _labels {nullptr};
    Operands _operands;

    explicit LabelExpr(Kind kind);
    ~LabelExpr();

    void addConjunct(LabelExpr* conjunct);
    void addDisjunct(LabelExpr* disjunct);
};

}
