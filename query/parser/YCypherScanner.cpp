#include "YCypherScanner.h"

#include "ParserException.h"
#include "CypherError.h"

using namespace db;

bool YCypherScanner::subtractsFromTheLastToken() const {
    switch (_lastToken) {
        // A value, a name, or the close of one: what stands before a subtraction
        case YCypherParser::token::DIGIT:
        case YCypherParser::token::DOUBLE:
        case YCypherParser::token::STRING_LITERAL:
        case YCypherParser::token::TRUE:
        case YCypherParser::token::FALSE:
        case YCypherParser::token::ID:
        case YCypherParser::token::ESC_LITERAL:
        case YCypherParser::token::CPAREN:
        case YCypherParser::token::CBRACK:
        case YCypherParser::token::CBRACE:
            return true;
        break;

        default:
            return false;
        break;
    }
}

void YCypherScanner::syntaxError(const SourceLocation& loc,
                                 const std::string& msg) {
    std::string errorMsg;

    CypherError err(_query);
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

void YCypherScanner::notImplemented(const SourceLocation& loc,
                                    std::string_view rawMsg) {
    std::string msg = fmt::format("Not implemented: {}", rawMsg);
    std::string errorMsg;

    CypherError err(_query);
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

