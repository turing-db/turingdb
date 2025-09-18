#include "QueryParser.h"

#include <sstream>
#include <limits>
#include <string>

#include "ParserException.h"
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
        throw ParserException(std::move(errMsg));
    }

    return _astCtxt->getRoot();
}

std::string sanitizeJsonString(const std::string& input) {
    std::string result;
    result.reserve(input.size() * 1.2); // Pre-allocate

    for (char c : input) {
        if (c == '"') {
            result += "\\\"";
        } else if (c == '\\') {
            result += "\\\\";
        } else if (c == '/') {
            result += "\\/";
        } else {
            result += c;
        }
    }

    return result;
}

void QueryParser::generateErrorMsg(std::string& msg,
                                   const std::string_view query,
                                   const std::string_view excptMsg,
                                   const location& loc) {
    std::ostringstream out;
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

    out << "\\n"
        << " |" << "\\n";
    out << errLineNo << "|\\t" << sanitizeJsonString(errLine) << "\\n";
    out << " |\\t" << blank << errorBars << "\\t";
    out << "\\t" << excptMsg << "\\n\\n";

    out << "Parse error on line " << loc.begin.line 
        << ", from column " << loc.begin.column 
        << ", to column " << loc.end.column;

    msg = out.str();
}
