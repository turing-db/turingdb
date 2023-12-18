#include "QueryParser.h"

#include <sstream>

#include "YScanner.h"
#include "parser.hpp"
#include "ASTContext.h"

using namespace db;

QueryParser::QueryParser(ASTContext* astCtxt)
    : _astCtxt(astCtxt)
{
}

QueryCommand* QueryParser::parse(StringSpan query) {
    YScanner yscanner;
    YParser yparser(yscanner, _astCtxt);

    std::istringstream iss;
    iss.rdbuf()->pubsetbuf(query.getData(), query.getSize());

    yscanner.switch_streams(&iss, NULL);

    yparser.parse();

    return _astCtxt->getRoot();
}
