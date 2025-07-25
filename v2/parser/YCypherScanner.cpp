#include "YCypherScanner.h"

#include "ParserException.h"
#include "CypherError.h"

void db::YCypherScanner::syntaxError(const SourceLocation& loc,
                                     const std::string& msg) {
    std::string errorMsg;

    CypherError err {_query};
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

void db::YCypherScanner::notImplemented(const SourceLocation& loc,
                                        std::string_view rawMsg) {
    if (_allowNotImplemented) {
        return;
    }

    std::string msg = fmt::format("Not implemented: {}", rawMsg);
    std::string errorMsg;

    CypherError err {_query};
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

