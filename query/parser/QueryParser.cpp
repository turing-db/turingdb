#include "QueryParser.h"

#include <sstream>
#include <limits>
#include <string>

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
        std::string errMsg;
        generateErrorMsg(errMsg, query, e.what(), e.location);
        std::cout << errMsg << std::endl;

        return nullptr;
    }
    
    return _astCtxt->getRoot();
}

void QueryParser::generateErrorMsg(std::string& msg,
                          const std::string_view query,
                          const std::string_view excptMsg,
                          const location& loc){
    // Get error line of query
    const uint32_t errLineNo = loc.begin.line;
    std::istringstream ss(std::string(query), std::ios_base::in);
    for (size_t i{1}; i < errLineNo; i++) { // Location is 1-indexed
        ss.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    std::string errLine;
    std::getline(ss, errLine);

    const size_t errLen = loc.end.column - loc.begin.column;
    const std::string errorBars(errLen, '^');
    const std::string blank(loc.begin.column - 1, ' ');

    std::stringstream out;
    out << " |" << '\n';
    out << errLineNo << "|\t" << errLine << '\n';
    out << " |\t" << blank << errorBars << '\n';

    out << excptMsg << std::endl;

    msg = out.str();
}
