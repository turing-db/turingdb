%{

#include <charconv>
#include <stdlib.h>

#include "YCypherScanner.h"
#include "GeneratedCypherParser.h"

using namespace db;

#define yyterminate() return db::YCypherParser::token::PROG_END
#define YY_USER_ACTION advanceLocation(*yylloc, yyleng);

using Token = db::YCypherParser::token;

#define STORE_STRING(start, length) yylval->emplace<std::string_view>(getStringView(start, length))

#define KEYWORD(name) STORE_STRING(_offset, yyleng); return Token::name;

%}

%option caseless noyywrap c++ yyclass="YCypherScanner"

%%

"//"[^\n]* { }

\n        { locationNewLine(*yylloc); }
[ \t\r]+  { }

"-"[ \t\r]*-  { return Token::TAIL_TAIL; }
"<"[ \t\r]*-[ \t\r]*- { return Token::TIP_TAIL_TAIL; }
"-"[ \t\r]*-[ \t\r]*> { return Token::TAIL_TAIL_TIP; }
"<"[ \t\r]*-[ \t\r]*\[ { return Token::TIP_TAIL_BRACKET; }
"]"[ \t\r]*-[ \t\r]*> { return Token::BRACKET_TAIL_TIP; }
"-"[ \t\r]*\[  { return Token::TAIL_BRACKET; }
"]"[ \t\r]*-  { return Token::BRACKET_TAIL; }

"+=" { return Token::ADD_ASSIGN; }
"<>" { return Token::NOT_EQUAL; }
"!=" { return Token::NOT_EQUAL; }
".." { return Token::RANGE; }
"<=" { return Token::LE; }
">=" { return Token::GE; }

";"  { return Token::SEMI_COLON; }
"("  { return Token::OPAREN; }
")"  { return Token::CPAREN; }
"{"  { return Token::OBRACE; }
"}"  { return Token::CBRACE; }
"["  { return Token::OBRACK; }
"]"  { return Token::CBRACK; }
"="  { return Token::ASSIGN; }
"$"  { return Token::DOLLAR; }
","  { return Token::COMMA; }
":"  { return Token::COLON; }
"^"  { return Token::CARET; }
"|"  { return Token::PIPE; }
"+"  { return Token::PLUS; }
"*"  { return Token::MULT; }
"`"  { return Token::ESC; }
"-"  { return Token::SUB; }
"."  { return Token::DOT; }
"/"  { return Token::DIV; }
"%"  { return Token::MOD; }
">"  { return Token::GT; }
"<"  { return Token::LT; }


"MERGE_DATAPARTS" { KEYWORD(MERGE_DATAPARTS) }
"EXTENSIONS" { KEYWORD(EXTENSIONS) }
"PROCEDURES" { KEYWORD(PROCEDURES) }
"SHORTESTPATH" { KEYWORD(SHORTESTPATH) }
"DESCENDING" { KEYWORD(DESCENDING) }
"DIMENSION" { KEYWORD(DIMENSION) }
"CONSTRAINT" { KEYWORD(CONSTRAINT) }
"COSINE" { KEYWORD(COSINE) }
"EUCLID" { KEYWORD(EUCLID) }
"HNSW"   { KEYWORD(HNSW) }
"FLAT"   { KEYWORD(FLAT) }
"TYPE"   { KEYWORD(TYPE) }
"MANDATORY" { KEYWORD(MANDATORY) }
"ASCENDING" { KEYWORD(ASCENDING) }
"OPTIONAL" { KEYWORD(OPTIONAL) }
"CONTAINS" { KEYWORD(CONTAINS) }
"DISTINCT" { KEYWORD(DISTINCT) }
"EXPLAIN" { KEYWORD(EXPLAIN) }
"EXTRACT" { KEYWORD(EXTRACT) }
"REQUIRE" { KEYWORD(REQUIRE) }
"COLLECT" { KEYWORD(COLLECT) }
"CONNECT" { KEYWORD(CONNECT) }
"SUBMIT" { KEYWORD(SUBMIT) }
"CHANGE" { KEYWORD(CHANGE) }
"COMMIT" { KEYWORD(COMMIT) }
"STARTS" { KEYWORD(STARTS) }
"UNIQUE" { KEYWORD(UNIQUE) }
"FILTER" { KEYWORD(FILTER) }
"SINGLE" { KEYWORD(SINGLE) }
"SCALAR" { KEYWORD(SCALAR) }
"UNWIND" { KEYWORD(UNWIND) }
"REMOVE" { KEYWORD(REMOVE) }
"RETURN" { KEYWORD(RETURN) }
"SEARCH" { KEYWORD(SEARCH) }
"CREATE" { KEYWORD(CREATE) }
"DELETE" { KEYWORD(DELETE) }
"DETACH" { KEYWORD(DETACH) }
"EXISTS" { KEYWORD(EXISTS) }
"IS NOT" { KEYWORD(IS_NOT) }
"LIMIT" { KEYWORD(LIMIT) }
"YIELD" { KEYWORD(YIELD) }
"MATCH" { KEYWORD(MATCH) }
"MERGE" { KEYWORD(MERGE) }
"METRIC" { KEYWORD(METRIC) }
"ORDER" { KEYWORD(ORDER) }
"WHERE" { KEYWORD(WHERE) }
"UNION" { KEYWORD(UNION) }
"VECTOR" { KEYWORD(VECTOR) }
"EMBEDDING" { KEYWORD(EMBEDDING) }
"FALSE" { KEYWORD(FALSE) }
"GRAPH" { KEYWORD(GRAPH) }
"GRAPHS" { KEYWORD(GRAPHS) }
"AVAILABLE" { KEYWORD(AVAILABLE) }
"INDEX" { KEYWORD(INDEX) }
"INDEXES" { KEYWORD(INDEXES) }
"COUNT"/[ \t\r\n]*[({] { KEYWORD(COUNT) }
"HEADERS" { KEYWORD(HEADERS) }
"EMBEDDINGS" { KEYWORD(EMBEDDINGS) }
"JSONL" { KEYWORD(JSONL) }
"LIST" { KEYWORD(LIST) }
"DESC" { KEYWORD(DESC) }
"CALL" { KEYWORD(CALL) }
"NULL" { KEYWORD(NULL_) }
"TRUE" { KEYWORD(TRUE) }
"WHEN" { KEYWORD(WHEN) }
"NONE" { KEYWORD(NONE) }
"THEN" { KEYWORD(THEN) }
"ELSE" { KEYWORD(ELSE) }
"CASE" { KEYWORD(CASE) }
"ERROR" { KEYWORD(ERROR_) }
"ENDS" { KEYWORD(ENDS) }
"DROP" { KEYWORD(DROP) }
"SHOW" { KEYWORD(SHOW) }
"FAIL" { KEYWORD(FAIL) }
"SKIP" { KEYWORD(SKIP) }
"WITH" { KEYWORD(WITH) }
"LOAD" { KEYWORD(LOAD) }
"INSTALL" { KEYWORD(INSTALL) }
"PUSH" { KEYWORD(PUSH) }
"PULL" { KEYWORD(PULL) }
"NEW" { KEYWORD(NEW) }
"CSV" { KEYWORD(CSV) }
"GML" { KEYWORD(GML) }
"PARQUET" { KEYWORD(PARQUET) }
"ANY" { KEYWORD(ANY) }
"SET" { KEYWORD(SET) }
"ALL" { KEYWORD(ALL) }
"ASC" { KEYWORD(ASC) }
"NOT" { KEYWORD(NOT) }
"END" { KEYWORD(END) }
"XOR" { KEYWORD(XOR) }
"FOR" { KEYWORD(FOR) }
"FROM" { KEYWORD(FROM) }
"ADD" { KEYWORD(ADD) }
"AND" { KEYWORD(AND) }
"OR" { KEYWORD(OR) }
"||" { KEYWORD(OR) }
"IN" { KEYWORD(IN) }
"IS" { KEYWORD(IS) }
"S3" { KEYWORD(S3) }
"BY" { KEYWORD(BY) }
"DO" { KEYWORD(DO) }
"OF" { KEYWORD(OF) }
"ON" { KEYWORD(ON) }
"IF" { KEYWORD(IF) }
"AS" { KEYWORD(AS) }


`[^`]*` {
    /* Escaped identifier. Example: `my-var` */
    STORE_STRING(_offset + 1, yyleng - 2);
    return Token::ESC_LITERAL;
}


\"([^\"\\\r\n]|\\.)*\" {
    /* String literal */
    STORE_STRING(_offset + 1, yyleng - 2);
    return Token::STRING_LITERAL;
}

\'([^\'\\\r\n]|\\.)*\' {
    /* Single-quoted string literal */
    STORE_STRING(_offset + 1, yyleng - 2);
    return Token::STRING_LITERAL;
}

-?[0-9]*\.[0-9]+([eE][+-]?[0-9]+)?[fd]? {
    /* Double pattern */
    const double v = strtod(yytext, nullptr);
    yylval->emplace<double>(v);

    return Token::DOUBLE;
}


-?[0-9]+[eE][+-]?[0-9]+[fd]? {
    /* Double pattern */
    const double v = strtod(yytext, nullptr);
    yylval->emplace<double>(v);

    return Token::DOUBLE;
}


-?[0-9]+[fd] {
    const double v = strtod(yytext, nullptr);
    yylval->emplace<double>(v);

    return Token::DOUBLE;
}


    /*
     * Hexadecimal literals.
     * from_chars using base 16 does not strip the leading 0x,
     * so + 2 to the token src location, or + 3 if there is leading '-'.
     * yytext will always be at least 2/3 characters due to the
     * below regex, so it is always safe to + 2/3.
     * We also need to manually negate the resultant value.
     * NOTE: Current implementation disallows literals equal to INT_MIN,
     * because it requires more complex processing. Values in the range
     * [INT_MIN + 1, INT_MAX] are allowed.
     */
-?0[xX][0-9a-fA-F]+ {
    static constexpr int HEX_BASE = 16;

    const char* begin = yytext;
    const char* end = yytext + yyleng;

    const bool negative = *begin == '-';
    if (negative) {
        begin += 3; // skip leading "-0x"
    } else {
        begin += 2; // skip leading "0x"
    }

    int64_t v = 0;
    const auto [_, err] = std::from_chars(begin, end, v, HEX_BASE);
    if (err == std::errc::result_out_of_range) {
        syntaxError(*yylloc, "Hexadecimal literal exceeded int64 range.");
    }

    if (negative) {
        v = -v;
    }

    yylval->emplace<int64_t>(v);

    return Token::DIGIT;
}


    /*
     * Octal literals.
     * from_chars using base 8 does not strip the leading 0o,
     * so + 2 to the token src location, or + 3 if there is leading '-'.
     * yytext will always be at least 2/3 characters due to the
     * below regex, so it is always safe to + 2/3.
     * We also need to manually negate the resultant value.
     * NOTE: Current implementation disallows literals equal to INT_MIN,
     * because it requires more complex processing. Values in the range
     * [INT_MIN + 1, INT_MAX] are allowed.
     */
-?0[oO][0-7]+ {
    static constexpr int OCT_BASE = 8;

    const char* begin = yytext;
    const char* end = yytext + yyleng;

    const bool negative = *begin == '-';
    if (negative) {
        begin += 3; // skips leading "-0o"
    } else {
        begin += 2; // skips leading "0o"
    }

    int64_t v = 0;
    const auto [_, err] = std::from_chars(begin, end, v, OCT_BASE);

    if (err == std::errc::result_out_of_range) {
        syntaxError(*yylloc, "Octal literal exceeded int64 range.");
    }

    if (negative) {
        v = -v;
    }

    yylval->emplace<int64_t>(v);

    return Token::DIGIT;
}


    /*
     * Decimal literals; no prefix required.
     * from_chars automatically strips leading '-' for decimal,
     * and handles negation if present. Allows literals in the
     * range [INT_MIN, INT_MAX].
     */
-?[0-9]+ {
    static constexpr int DEC_BASE = 10;

    const char* begin = yytext;
    const char* end = yytext + yyleng;

    int64_t v = 0;
    const auto [_, err] = std::from_chars(begin, end, v, DEC_BASE);

    if (err == std::errc::result_out_of_range) {
        syntaxError(*yylloc, "Decimal literal exceeded int64 range.");
    }

    yylval->emplace<int64_t>(v);

    return Token::DIGIT;
}


[a-zA-Z_][a-zA-Z0-9_]* {
    /* Identifier */
    STORE_STRING(_offset, yyleng);
    return Token::ID;
}


. { return YCypherParser::token::UNKNOWN; }


%%
