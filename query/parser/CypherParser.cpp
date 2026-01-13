#include "CypherParser.h"

#include <sstream>

#include "CypherAST.h"
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
    std::istringstream iss{std::string{query}};

    yscanner.switch_streams(&iss, nullptr);
    yparser.parse();
}
