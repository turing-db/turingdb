#include "CypherASTDumper.h"

#include <vector>

#include "Parameter.h"
#include "SymbolChain.h"
#include "LabelExpr.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "WhereClause.h"
#include "NodePattern.h"
#include "EdgePattern.h"
#include "Symbol.h"
#include "Literal.h"
#include "decl/VarDecl.h"
#include "QualifiedName.h"
#include "FunctionInvocation.h"

#include "expr/All.h"

#include "CompilerException.h"

using namespace db;

namespace {

std::string sanitizeString(std::string_view str) {
    std::string result = "_";
    std::transform(str.begin(), str.end(), std::back_inserter(result), [](char c) {
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            return c;
        }

        return '_';
    });

    return result;
}

void dumpLabelExpr(std::ostream& out, const LabelExpr* labelExpr) {
    std::vector<const LabelExpr*> worklist {labelExpr};

    while (!worklist.empty()) {
        const LabelExpr* expr = worklist.back();
        worklist.pop_back();

        const LabelExpr::Operands& operands = expr->getOperands();
        worklist.insert(worklist.end(), operands.rbegin(), operands.rend());

        switch (expr->getKind()) {
            case LabelExpr::Kind::LABELS: {
                const SymbolChain* labels = expr->getLabels();
                for (const Symbol* label : *labels) {
                    out << "        Label " << label->getName() << "\n";
                }

                for (const Parameter* parameter : labels->getParameters()) {
                    out << "        LabelParameter $" << parameter->getName() << "\n";
                }
            }
            break;
            case LabelExpr::Kind::NOT:
                out << "        LabelNot " << std::dec << operands.size() << "\n";
            break;
            case LabelExpr::Kind::AND:
                out << "        LabelAnd " << std::dec << operands.size() << "\n";
            break;
            case LabelExpr::Kind::OR:
                out << "        LabelOr " << std::dec << operands.size() << "\n";
            break;
        }
    }
}

void dumpQualifiedName(std::ostream& out, const QualifiedName* name) {
    size_t i = 0;
    for (const Symbol* sym : name->names()) {
        if (i != 0) {
            out << "_";
        }

        out << sym->getName();
        i++;
    }
}

}

void CypherASTDumper::dump(std::ostream& out, const Pattern* pattern) {
    out << "    _" << std::hex << pattern << " {\n";
    out << "        ASTType Pattern\n";
    out << "    }\n";

    for (const PatternElement* part : pattern->elements()) {
        out << "    _" << std::hex << pattern << " ||--o{ _" << std::hex << part << " : \"\"\n";
        dump(out, part);
    }

    if (const WhereClause* where = pattern->getWhere()) {
        out << "    _" << std::hex << pattern << " ||--o{ _" << std::hex << where << " : \"\"\n";
        dump(out, where);
    }
}

void CypherASTDumper::dump(std::ostream& out, const PatternElement* elem) {
    out << "    _" << std::hex << elem << " {\n";
    out << "        ASTType PatternElement\n";
    out << "    }\n";

    for (const auto& entity : elem->getEntities()) {
        out << "    _" << std::hex << elem << " ||--o{ _" << std::hex << entity << " : \"\"\n";
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            dump(out, node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            dump(out, edge);
        }
    }
}

void CypherASTDumper::dump(std::ostream& out, const WhereClause* where) {
    out << "    _" << std::hex << where << " {\n";
    out << "        ASTType WHERE\n";
    out << "    }\n";

    const Expr* expr = where->getExpr();
    out << "    _" << std::hex << where << " ||--o{ _" << std::hex << expr << " : \"\"\n";
    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const NodePattern* node) {
    out << "    _" << std::hex << node << " {\n";
    out << "        ASTType NodePattern\n";

    if (const Symbol* symbol = node->getSymbol()) {
        out << "        Symbol " << symbol->getName() << "\n";
    }

    if (const LabelExpr* labelExpr = node->getLabelExpr()) {
        dumpLabelExpr(out, labelExpr);
    }

    if (const MapLiteral* properties = node->getProperties()) {
        for (const auto& [k, v] : *properties) {
            out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }\n";

    const VarDecl* decl = node->getDecl();
    if (decl) {
        out << "    _" << std::hex << node << " ||--o{ VAR_" << decl << " : \"\"\n";
        dump(out, decl);
    }
}

void CypherASTDumper::dump(std::ostream& out, const EdgePattern* edge) {
    out << "    _" << std::hex << edge << " {\n";
    out << "        ASTType EdgePattern\n";

    if (Symbol* symbol = edge->getSymbol()) {
        out << "        Name " << symbol->getName() << "\n";
    }

    if (const LabelExpr* typeExpr = edge->getLabelExpr()) {
        dumpLabelExpr(out, typeExpr);
    }

    if (const MapLiteral* properties = edge->getProperties()) {
        for (const auto& [k, v] : *properties) {
            out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }\n";

    const VarDecl* decl = edge->getDecl();
    if (decl) {
        out << "    _" << std::hex << edge << " ||--o{ VAR_" << decl << " : \"\"\n";
        dump(out, decl);
    }
}

void CypherASTDumper::dump(std::ostream& out, const MapLiteral* map) {
    out << "    _" << std::hex << map << " {\n";
    out << "        ASTType MapLiteral\n";

    for (const auto& [k, v] : *map) {
        out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
    }

    out << "    }\n";

    for (const auto& [k, v] : *map) {
        out << "    _" << std::hex << map << " ||--o{ _" << std::hex << v << " : \"\"\n";
        dump(out, v);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            dump(out, dynamic_cast<const BinaryExpr*>(expr));
            break;
        case Expr::Kind::LOGICAL:
            dump(out, dynamic_cast<const LogicalExpr*>(expr));
            break;
        case Expr::Kind::UNARY:
            dump(out, dynamic_cast<const UnaryExpr*>(expr));
            break;
        case Expr::Kind::STRING:
            dump(out, dynamic_cast<const StringExpr*>(expr));
            break;
        case Expr::Kind::ENTITY_TYPES:
            dump(out, dynamic_cast<const EntityTypeExpr*>(expr));
            break;
        case Expr::Kind::PROPERTY:
            dump(out, dynamic_cast<const PropertyExpr*>(expr));
            break;
        case Expr::Kind::PROPERTY_LOOKUP:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType PropertyLookupExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::SYMBOL:
            dump(out, dynamic_cast<const SymbolExpr*>(expr));
            break;
        case Expr::Kind::LITERAL:
            dump(out, dynamic_cast<const LiteralExpr*>(expr));
            break;
        case Expr::Kind::PARAMETER:
            dump(out, dynamic_cast<const ParameterExpr*>(expr));
            break;
        case Expr::Kind::FUNCTION_INVOCATION:
            dump(out, dynamic_cast<const FunctionInvocationExpr*>(expr));
            break;
        case Expr::Kind::INDEX:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType IndexExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::LIST:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ListExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::LIST_SLICE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ListSliceExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::LIST_COMPREHENSION:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ListComprehensionExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType PatternComprehensionExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::LIST_PREDICATE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ListPredicateExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::REDUCE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ReduceExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::ALL_REDUCE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType AllReduceExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::MAP_PROJECTION:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType MapProjectionExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::CASE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType CaseExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::EXISTS:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType ExistsExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::COUNT_SUBQUERY:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType CountSubqueryExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::COLLECT_SUBQUERY:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType CollectSubqueryExpr\n";
            out << "    }\n";
            break;
        case Expr::Kind::_SIZE:
            out << "    _" << std::hex << expr << " {\n";
            out << "        ASTType __INVALIDEXPR__\n";
            out << "    }\n";
        break;
    }
}

void CypherASTDumper::dump(std::ostream& out, const BinaryExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType BinaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    switch (expr->getOperator()) {
        case BinaryOperator::NotEqual:
            out << "        Operator NOT_EQUAL\n";
            break;
        case BinaryOperator::Equal:
            out << "        Operator EQUAL\n";
            break;
        case BinaryOperator::LessThan:
            out << "        Operator LESS_THAN\n";
            break;
        case BinaryOperator::GreaterThan:
            out << "        Operator GREATER_THAN\n";
            break;
        case BinaryOperator::LessThanOrEqual:
            out << "        Operator LESS_THAN_OR_EQUAL\n";
            break;
        case BinaryOperator::GreaterThanOrEqual:
            out << "        Operator GREATER_THAN_OR_EQUAL\n";
            break;
        case BinaryOperator::Add:
            out << "        Operator ADD\n";
            break;
        case BinaryOperator::Sub:
            out << "        Operator SUB\n";
            break;
        case BinaryOperator::Mult:
            out << "        Operator MULT\n";
            break;
        case BinaryOperator::Div:
            out << "        Operator DIV\n";
            break;
        case BinaryOperator::Mod:
            out << "        Operator MOD\n";
            break;
        case BinaryOperator::Pow:
            out << "        Operator POW\n";
            break;
        case BinaryOperator::In:
            out << "        Operator IN\n";
            break;
        case BinaryOperator::IsNull:
            out << "        Operator IS_NULL\n";
            break;
        case BinaryOperator::IsNotNull:
            out << "        Operator IS_NOT_NULL\n";
            break;
        default:
            throw CompilerException("Unknown binary operator");
            break;
    }

    out << "    }\n";

    const Expr* left = expr->getLHS();
    const Expr* right = expr->getRHS();

    out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << left << " : \"\"\n";

    dump(out, left);

    out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << right << " : \"\"\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const LogicalExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType LogicalExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";
    out << "        Operator " << LogicalOperatorDescription::value(expr->getOperator()) << "\n";
    out << "    }\n";

    for (const Expr* operand : expr->getOperands()) {
        out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << operand << " : \"\"\n";

        dump(out, operand);
    }
}

void CypherASTDumper::dump(std::ostream& out, const UnaryExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType UnaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    switch (expr->getOperator()) {
        case UnaryOperator::Not:
            out << "        Operator NOT\n";
            break;
        case UnaryOperator::Minus:
            out << "        Operator MINUS\n";
            break;
        case UnaryOperator::Plus:
            out << "        Operator PLUS\n";
            break;
        default:
            throw CompilerException("Unknown unary operator");
            break;
    }

    out << "    }\n";

    const Expr* right = expr->getSubExpr();

    out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << right << " : \"\"\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const SymbolExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType SymbolExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";
    out << "        Symbol " << expr->getSymbol()->getName() << "\n";
    out << "    }\n";

    if (VarDecl* decl = expr->getDecl()) {
        out << "    _" << std::hex << expr << " ||--o{ VAR_" << decl << " : \"\"\n";
    }
}

void CypherASTDumper::dump(std::ostream& out, const ParameterExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType ParameterExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";
    out << "        Name " << expr->getParameter()->getName() << "\n";
    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const LiteralExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType LiteralExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    const Literal* literal = expr->getLiteral();
    switch (literal->getKind()) {
        case Literal::Kind::NULL_LITERAL: {
            out << "        NullLiteral _\n";
            break;
        }
        case Literal::Kind::BOOL: {
            const BoolLiteral* boolLiteral = dynamic_cast<const BoolLiteral*>(literal);
            out << "        BoolLiteral _" << boolLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::INTEGER: {
            const IntegerLiteral* integerLiteral = dynamic_cast<const IntegerLiteral*>(literal);
            out << "        IntLiteral _" << integerLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* doubleLiteral = dynamic_cast<const DoubleLiteral*>(literal);
            out << "        DoubleLiteral _" << doubleLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::STRING: {
            const StringLiteral* stringLiteral = dynamic_cast<const StringLiteral*>(literal);
            out << "        StringLiteral '" << stringLiteral->getValue() << "'\n";
            break;
        }
        case Literal::Kind::CHAR: {
            const CharLiteral* charLiteral = dynamic_cast<const CharLiteral*>(literal);
            out << "        CharLiteral '" << charLiteral->getValue() << "'\n";
            break;
        }
        case Literal::Kind::LIST: {
            const ListLiteral* listLiteral = dynamic_cast<const ListLiteral*>(literal);
            out << "        ListLiteral _" << listLiteral << "\n";
            break;
        }
        case Literal::Kind::MAP: {
            const MapLiteral* mapLiteral = dynamic_cast<const MapLiteral*>(literal);
            out << "        MapLiteral _" << mapLiteral << "\n";
            break;
        }
        case Literal::Kind::EMBEDDING: {
            const EmbeddingLiteral* embLiteral = dynamic_cast<const EmbeddingLiteral*>(literal);
            out << "        EmbeddingLiteral [";
            const auto data = embLiteral->getValue();
            for (size_t i = 0; i < data.size(); i++) {
                if (i > 0) out << ", ";
                out << data[i];
            }
            out << "]\n";
            break;
        }
        case Literal::Kind::NORMAL_FORM: {
            const NormalFormLiteral* normalFormLiteral = static_cast<const NormalFormLiteral*>(literal);
            out << "        NormalFormLiteral " << UnicodeNormalFormName::value(normalFormLiteral->getForm()) << "\n";
            break;
        }
        default: {
            throw CompilerException("Unknown literal type");
            break;
        }
    }

    out << "    }\n";
}

void CypherASTDumper::dump(std::ostream& out, const EntityTypeExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType EntityTypeExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    dumpLabelExpr(out, expr->getLabelExpr());

    out << "    }\n";

    if (const Expr* operand = expr->getOperand()) {
        out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << operand << " : \"\"\n";
        dump(out, operand);
    } else if (const VarDecl* entityDecl = expr->getEntityVarDecl()) {
        out << "    _" << std::hex << expr << " ||--o{ VAR_" << entityDecl << " : \"\"\n";
    }
}

void CypherASTDumper::dump(std::ostream& out, const StringExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType StringExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    switch (expr->getStringOperator()) {
        case StringOperator::StartsWith:
            out << "        Operator STARTS_WITH\n";
            break;
        case StringOperator::EndsWith:
            out << "        Operator ENDS_WITH\n";
            break;
        case StringOperator::Contains:
            out << "        Operator CONTAINS\n";
            break;
        default:
            throw CompilerException("Unknown string operator");
            break;
    }

    out << "    }\n";

    const Expr* left = expr->getLHS();
    const Expr* right = expr->getRHS();

    out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << right << " : \"\"\n";

    out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << left << " : \"\"\n";

    dump(out, left);
    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const PropertyExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType PropertyExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    out << "        QualifiedName ";

    dumpQualifiedName(out, expr->getFullName());

    out << "\n    }\n";

    if (!expr->getEntityVarDecl()) {
        return;
    }

    out << "    _" << std::hex << expr << " ||--o{ VAR_" << expr->getEntityVarDecl() << " : \"\"\n";
}

void CypherASTDumper::dump(std::ostream& out, const FunctionInvocationExpr* expr) {
    out << "    _" << std::hex << expr << " {\n";
    out << "        ASTType FunctionInvocationExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    const FunctionInvocation* invocation = expr->getFunctionInvocation();
    out << "        FunctionName ";
    dumpQualifiedName(out, invocation->getName());
    out << "\n";

    if (invocation->isDistinct()) {
        out << "        Distinct true\n";
    }

    out << "    }\n";

    for (const Expr* arg : invocation->getArguments()->getExprs()) {
        out << "    _" << std::hex << expr << " ||--o{ _" << std::hex << arg << " : \"\"\n";
        dump(out, arg);
    }
}

void CypherASTDumper::dump(std::ostream& out, const VarDecl* decl) {
    if (_dumpedVariables.contains(decl)) {
        return;
    }

    out << "    VAR_" << decl << " {\n";
    out << "        ValueType " << EvaluatedTypeName::value(decl->getType()) << "\n";

    const auto& name = decl->getName();

    if (!name.empty()) {
        out << "        Name " << name << "\n";
    }

    out << "    }\n";

    _dumpedVariables.insert(decl);
}
