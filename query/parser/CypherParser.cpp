#include "CypherParser.h"

#include <sstream>
#include <stdint.h>

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "SourceLocation.h"
#include "SourceManager.h"
#include "expr/ExprDepth.h"
#include "YCypherScanner.h"

using namespace db;

CypherParser::CypherParser(CypherAST* ast)
    : _ast(ast)
{
}

void CypherParser::parse(std::string_view query) {
    YCypherScanner yscanner;
    yscanner.setQuery(query);

    YCypherParser yparser(yscanner, _ast);

    // Use string constructor for istringstream to ensure proper buffer setup
    // Note: pubsetbuf is a no-op on libc++ (macOS) for string streams
    std::istringstream iss({std::string(query)});

    yscanner.switch_streams(&iss, nullptr);
    yparser.parse();

    const Expr* tooDeep = ExprDepth::findDeeperThan(_ast->getExpressions(), MAX_EXPRESSION_DEPTH);
    if (tooDeep) {
        const SourceManager* sourceManager = _ast->getSourceManager();
        const SourceLocation* location = sourceManager->getLocation(reinterpret_cast<uintptr_t>(tooDeep));

        yscanner.syntaxError(location ? *location : SourceLocation {},
                             fmt::format("Expression nested deeper than {} levels", MAX_EXPRESSION_DEPTH));
    }
}
