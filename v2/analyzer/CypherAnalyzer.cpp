#include "CypherAnalyzer.h"

#include "AnalyzeException.h"
#include "CypherAST.h"

#include "attribution/DeclContext.h"
#include "attribution/VariableType.h"
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
            continue;
        }

        throw AnalyzeException("Unsupported query type");
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery& query) {
    for (const auto* statement : query.getStatements()) {
        if (const auto* s = dynamic_cast<const Match*>(statement)) {
            analyze(*s);
            continue;
        }

        if (const auto* s = dynamic_cast<const Return*>(statement)) {
            analyze(*s);
            continue;
        }

        throw AnalyzeException("Unsupported statement type");
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
    for (const auto& element : pattern.elements()) {
        analyze(*element);
    }
}

void CypherAnalyzer::analyze(const PatternElement& element) {
    const auto& entities = element.getEntities();

    for (const auto& entity : entities) {
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(*node);
            continue;
        }

        if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(*edge);
            continue;
        }
    }
}

void CypherAnalyzer::analyze(NodePattern& node) {
    if (node.hasSymbol()) {
        VarDecl* var = _ctxt->getOrCreateNamedVariable(node.symbol()._name, VariableType::Node);
        node.setDecl(var);
    }
}

void CypherAnalyzer::analyze(EdgePattern& edge) {
}
