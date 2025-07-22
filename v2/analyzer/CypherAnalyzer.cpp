#include "CypherAnalyzer.h"

#include "AnalyzeException.h"
#include "CypherAST.h"

#include "attribution/DeclContext.h"
#include "attribution/VariableType.h"
#include "expressions/AtomExpression.h"
#include "expressions/BinaryExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "expressions/PathExpression.h"
#include "expressions/PropertyExpression.h"
#include "expressions/StringExpression.h"
#include "expressions/UnaryExpression.h"
#include "spdlog/fmt/bundled/core.h"
#include "types/SinglePartQuery.h"

#include "statements/Match.h"
#include "statements/Skip.h"
#include "statements/Limit.h"
#include "statements/Return.h"

using namespace db;

CypherAnalyzer::CypherAnalyzer(std::unique_ptr<CypherAST> ast)
    : _ast(std::move(ast)) {
}

CypherAnalyzer::~CypherAnalyzer() = default;

void CypherAnalyzer::analyze() {
    for (const auto& query : _ast->queries()) {
        _ctxt = &query->getRootContext();

        if (const auto* q = dynamic_cast<SinglePartQuery*>(query.get())) {
            analyze(*q);
        } else {
            throw AnalyzeException("Unsupported query type");
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery& query) {
    for (const auto* statement : query.getStatements()) {
        if (const auto* s = dynamic_cast<const Match*>(statement)) {
            fmt::print("Analyzing match statement\n");
            analyze(*s);
        }

        else if (const auto* s = dynamic_cast<const Return*>(statement)) {
            analyze(*s);
        } else {
            throw AnalyzeException("Unsupported statement type");
        }
    }
}

void CypherAnalyzer::analyze(const Match& matchSt) {
    if (matchSt.isOptional()) {
        throw AnalyzeException("OPTIONAL MATCH not supported");
    }

    const auto& pattern = matchSt.getPattern();
    analyze(pattern);

    if (matchSt.hasLimit()) {
        throw AnalyzeException("LIMIT not supported");
    }

    if (matchSt.hasSkip()) {
        throw AnalyzeException("SKIP not supported");
    }

    if (!matchSt.hasPattern()) {
        throw AnalyzeException("MATCH statement must have a pattern");
    }
}

void CypherAnalyzer::analyze(const Skip& skipSt) {
    throw AnalyzeException("SKIP not supported");
}

void CypherAnalyzer::analyze(const Limit& limitSt) {
    throw AnalyzeException("LIMIT not supported");
}

void CypherAnalyzer::analyze(const Return& returnSt) {
    throw AnalyzeException("RETURN not supported");
}

void CypherAnalyzer::analyze(const Pattern& pattern) {
    fmt::print("Analyzing pattern\n");
    for (const auto& element : pattern.elements()) {
        analyze(*element);
    }

    if (pattern.hasWhere()) {
        analyze(pattern.getWhere().getExpression());
    }

    fmt::print("Done analyzing pattern\n");
}

void CypherAnalyzer::analyze(const PatternElement& element) {
    fmt::print("Analyzing pattern element\n");
    const auto& entities = element.getEntities();

    for (const auto& entity : entities) {
        fmt::print("Analyzing pattern entity\n");
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(*node);
        } else if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(*edge);
        }
    }

    fmt::print("Done analyzing pattern element\n");
}

void CypherAnalyzer::analyze(NodePattern& node) {
    if (node.hasSymbol()) {
        fmt::print("Analyzing node pattern '{}'\n", node.symbol()._name);
        VarDecl* var = _ctxt->getOrCreateNamedVariable(node.symbol()._name, VariableType::Node);
        node.setDecl(var);
    } else {
        fmt::print("Analyzing node unnamed pattern\n");
        VarDecl* var = _ctxt->createUnnamedVariable(VariableType::Node);
        node.setDecl(var);
    }

    fmt::print("Done analyzing node pattern\n");
}

void CypherAnalyzer::analyze(EdgePattern& edge) {
    if (edge.hasSymbol()) {
        fmt::print("Analyzing edge pattern '{}'\n", edge.symbol()._name);
        VarDecl* var = _ctxt->getOrCreateNamedVariable(edge.symbol()._name, VariableType::Edge);
        edge.setDecl(var);
    } else {
        fmt::print("Analyzing edge unnamed pattern\n");
        VarDecl* var = _ctxt->createUnnamedVariable(VariableType::Edge);
        edge.setDecl(var);
    }

    fmt::print("Done analyzing edge pattern\n");
}

void CypherAnalyzer::analyze(Expression& expr) {
    fmt::print("Analyzing expression\n");
    if (auto* e = dynamic_cast<BinaryExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<UnaryExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<AtomExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<PropertyExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<StringExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<NodeLabelExpression*>(&expr)) {
        analyze(*e);
    } else if (auto* e = dynamic_cast<PathExpression*>(&expr)) {
        analyze(*e);
    }
}

void CypherAnalyzer::analyze(BinaryExpression& expr) {
    throw AnalyzeException("Binary expressions not supported");
}

void CypherAnalyzer::analyze(UnaryExpression& expr) {
    throw AnalyzeException("Unary expressions not supported");
}

void CypherAnalyzer::analyze(AtomExpression& expr) {
    throw AnalyzeException("Atom expressions not supported");
}

void CypherAnalyzer::analyze(PropertyExpression& expr) {
    throw AnalyzeException("Property expressions not supported");
}

void CypherAnalyzer::analyze(StringExpression& expr) {
    throw AnalyzeException("String expressions not supported");
}

void CypherAnalyzer::analyze(NodeLabelExpression& expr) {
    throw AnalyzeException("Node label expressions not supported");
}

void CypherAnalyzer::analyze(PathExpression& expr) {
    throw AnalyzeException("Path expressions not supported");
}
