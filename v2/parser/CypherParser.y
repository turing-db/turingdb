%require "3.2"
%language "c++"

%define api.parser.class { YCypherParser }
%define api.value.type variant
%define parse.assert
%define parse.error detailed
%define api.namespace { db }
%parse-param {db::YCypherScanner& scanner}
%locations

%header
%verbose

%code requires {
    #include <spdlog/fmt/bundled/core.h>

    namespace db {
        class YCypherScanner;
    }
}

%code top {
    #include "YCypherScanner.h"
    #include "GeneratedCypherParser.h"

    #undef yylex
    #define yylex scanner.lex
}

// General purpose tokens
%token UNKNOWN
%token END_TOKEN 0 // End of input
%token COLON
%token COMMA
%token 

%%

prog: END_TOKEN;

%%

void db::YCypherParser::error(const location_type& l, const std::string& m) {
    throw db::YCypherParser::syntax_error(l, m);
}


