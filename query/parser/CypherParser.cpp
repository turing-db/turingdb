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

    const Expr* tooDeep = ExprDepth::findDeeperThan(_ast->getExpressions(), MAX_EXPRESSION_DEPTH);
    if (tooDeep) {
        const SourceManager* sourceManager = _ast->getSourceManager();
        const SourceLocation* location = sourceManager->getLocation(reinterpret_cast<uintptr_t>(tooDeep));

        lexer.throwSyntaxError(location ? *location : SourceLocation {},
                               fmt::format("Expression nested deeper than {} levels", MAX_EXPRESSION_DEPTH));
    }
}
