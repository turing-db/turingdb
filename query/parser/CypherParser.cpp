#include "CypherParser.h"

#include <stdint.h>

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "CypherGrammar.h"
#include "CypherLexer.h"
#include "SourceLocation.h"
#include "SourceManager.h"
#include "expr/ExprDepth.h"

using namespace db;

CypherParser::CypherParser(CypherAST* ast)
    : _ast(ast)
{
}

void CypherParser::parse(std::string_view query) {
    CypherLexer lexer(query);
    CypherGrammar grammar(_ast, &lexer);
    grammar.parseScript();

    throwIfNestedTooDeep(lexer);
}

Expr* CypherParser::parseExpression(std::string_view expression, size_t& endOffset) {
    CypherLexer lexer(expression);
    CypherGrammar grammar(_ast, &lexer);
    Expr* expr = grammar.parseExpression(endOffset);

    throwIfNestedTooDeep(lexer);

    return expr;
}

void CypherParser::throwIfNestedTooDeep(CypherLexer& lexer) const {
    const Expr* tooDeep = ExprDepth::findDeeperThan(_ast->getExpressions(), MAX_EXPRESSION_DEPTH);
    if (tooDeep) {
        const SourceManager* sourceManager = _ast->getSourceManager();
        const SourceLocation* location = sourceManager->getLocation(reinterpret_cast<uintptr_t>(tooDeep));

        lexer.throwSyntaxError(location ? *location : SourceLocation {},
                               fmt::format("Expression nested deeper than {} levels", MAX_EXPRESSION_DEPTH));
    }
}
