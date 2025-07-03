#include "QueryParser.h"

#include <sstream>

#include "Profiler.h"
#include "YScanner.h"
#include "parser.hpp"
#include "ASTContext.h"

using namespace db;

QueryParser::QueryParser(ASTContext* astCtxt)
    : _astCtxt(astCtxt)
{
}

QueryCommand* QueryParser::parse(std::string_view query) {
    Profile profile {"QueryParser::parse"};

    YScanner yscanner;
    YParser yparser(yscanner, _astCtxt);

    std::istringstream iss;
    iss.rdbuf()->pubsetbuf((char*)query.data(), query.size());

    yscanner.switch_streams(&iss, NULL);

    try {
        yparser.parse();  
    } catch (const db::YParser::syntax_error& e) {
        std::cerr << e.what() << '\n';
        return nullptr;
    }
    
    return _astCtxt->getRoot();
}

