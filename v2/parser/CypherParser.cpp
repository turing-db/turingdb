#include "CypherParser.h"

#include <sstream>

#include "CypherAST.h"
#include "YCypherScanner.h"

using namespace db;

CypherParser::CypherParser(CypherAST& ast)
    : _ast(&ast)
{
}

void CypherParser::parse(std::string_view query) {
    YCypherScanner yscanner;
    yscanner.allowNotImplemented(_allowNotImplemented);
    yscanner.setQuery(query);

    YCypherParser yparser(yscanner, *_ast);

    std::istringstream iss;
    iss.rdbuf()->pubsetbuf((char*)query.data(), query.size());

    yscanner.switch_streams(&iss, NULL);
    yparser.parse();
}
