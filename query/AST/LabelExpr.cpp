#include "LabelExpr.h"

#include <algorithm>

#include "CypherAST.h"
#include "SymbolChain.h"

using namespace db;

namespace {

void appendLabels(SymbolChain* labels, const SymbolChain* appended) {
    for (Symbol* symbol : *appended) {
        labels->add(symbol);
    }

    for (Parameter* parameter : appended->getParameters()) {
        labels->addParameter(parameter);
    }
}

}

LabelExpr::LabelExpr(Kind kind)
    : _kind(kind)
{
}

LabelExpr::~LabelExpr() {
}

LabelExpr* LabelExpr::createLabels(CypherAST* ast, SymbolChain* labels) {
    LabelExpr* expr = new LabelExpr(Kind::LABELS);
    expr->_labels = labels;
    ast->addLabelExpr(expr);

    return expr;
}

LabelExpr* LabelExpr::createNot(CypherAST* ast, LabelExpr* operand) {
    if (operand->_kind == Kind::NOT) {
        return operand->_operands.front();
    }

    LabelExpr* expr = new LabelExpr(Kind::NOT);
    expr->_operands.push_back(operand);
    ast->addLabelExpr(expr);

    return expr;
}

LabelExpr* LabelExpr::createAnd(CypherAST* ast, LabelExpr* lhs, LabelExpr* rhs) {
    LabelExpr* conjunction = lhs;
    if (lhs->_kind != Kind::AND) {
        conjunction = new LabelExpr(Kind::AND);
        ast->addLabelExpr(conjunction);
        conjunction->addConjunct(lhs);
    }

    conjunction->addConjunct(rhs);

    if (conjunction->_operands.size() == 1) {
        return conjunction->_operands.front();
    }

    return conjunction;
}

LabelExpr* LabelExpr::createOr(CypherAST* ast, LabelExpr* lhs, LabelExpr* rhs) {
    LabelExpr* disjunction = lhs;
    if (lhs->_kind != Kind::OR) {
        disjunction = new LabelExpr(Kind::OR);
        ast->addLabelExpr(disjunction);
        disjunction->addDisjunct(lhs);
    }

    disjunction->addDisjunct(rhs);

    return disjunction;
}

bool LabelExpr::holdsParameter() const {
    std::vector<const LabelExpr*> worklist {this};

    while (!worklist.empty()) {
        const LabelExpr* expr = worklist.back();
        worklist.pop_back();

        if (expr->_kind != Kind::LABELS) {
            worklist.insert(worklist.end(), expr->_operands.begin(), expr->_operands.end());
        } else if (!expr->_labels->getParameters().empty()) {
            return true;
        }
    }

    return false;
}

void LabelExpr::addConjunct(LabelExpr* conjunct) {
    const auto labelSetIt = std::ranges::find_if(_operands, [](const LabelExpr* operand) {
        return operand->_kind == Kind::LABELS;
    });
    const bool joinsALabelSet = conjunct->_kind == Kind::LABELS && labelSetIt != _operands.end();

    if (conjunct->_kind == Kind::AND) {
        for (LabelExpr* operand : conjunct->_operands) {
            addConjunct(operand);
        }
    } else if (joinsALabelSet) {
        appendLabels((*labelSetIt)->_labels, conjunct->_labels);
    } else {
        _operands.push_back(conjunct);
    }
}

void LabelExpr::addDisjunct(LabelExpr* disjunct) {
    if (disjunct->_kind == Kind::OR) {
        _operands.insert(_operands.end(), disjunct->_operands.begin(), disjunct->_operands.end());
    } else {
        _operands.push_back(disjunct);
    }
}
