%{

#include <charconv>

#include "YCypherScanner.h"
#include "GeneratedCypherParser.h"

using namespace db::v2;

#define yyterminate() return db::v2::YCypherParser::token::PROG_END
#define YY_USER_ACTION advanceLocation(*yylloc, yyleng);

using Token = db::v2::YCypherParser::token;

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


"DESCENDING" { KEYWORD(DESCENDING) }
"CONSTRAINT" { KEYWORD(CONSTRAINT) }
"MANDATORY" { KEYWORD(MANDATORY) }
"ASCENDING" { KEYWORD(ASCENDING) }
"OPTIONAL" { KEYWORD(OPTIONAL) }
"CONTAINS" { KEYWORD(CONTAINS) }
"DISTINCT" { KEYWORD(DISTINCT) }
"EXTRACT" { KEYWORD(EXTRACT) }
"REQUIRE" { KEYWORD(REQUIRE) }
"COLLECT" { KEYWORD(COLLECT) }
"SUBMIT" { KEYWORD(SUBMIT) }
"CHANGE" { KEYWORD(CHANGE) }
"STARTS" { KEYWORD(STARTS) }
"UNIQUE" { KEYWORD(UNIQUE) }
"FILTER" { KEYWORD(FILTER) }
"SINGLE" { KEYWORD(SINGLE) }
"SCALAR" { KEYWORD(SCALAR) }
"UNWIND" { KEYWORD(UNWIND) }
"REMOVE" { KEYWORD(REMOVE) }
"RETURN" { KEYWORD(RETURN) }
"CREATE" { KEYWORD(CREATE) }
"DELETE" { KEYWORD(DELETE) }
"DETACH" { KEYWORD(DETACH) }
"EXISTS" { KEYWORD(EXISTS) }
"IS NOT" { KEYWORD(IS_NOT) }
"LIMIT" { KEYWORD(LIMIT) }
"YIELD" { KEYWORD(YIELD) }
"MATCH" { KEYWORD(MATCH) }
"MERGE" { KEYWORD(MERGE) }
"ORDER" { KEYWORD(ORDER) }
"WHERE" { KEYWORD(WHERE) }
"UNION" { KEYWORD(UNION) }
"FALSE" { KEYWORD(FALSE) }
"GRAPH" { KEYWORD(GRAPH) }
"COUNT" { KEYWORD(COUNT) }
"DESC" { KEYWORD(DESC) }
"CALL" { KEYWORD(CALL) }
"NULL" { KEYWORD(NULL_) }
"TRUE" { KEYWORD(TRUE) }
"WHEN" { KEYWORD(WHEN) }
"NONE" { KEYWORD(NONE) }
"THEN" { KEYWORD(THEN) }
"ELSE" { KEYWORD(ELSE) }
"CASE" { KEYWORD(CASE) }
"ENDS" { KEYWORD(ENDS) }
"DROP" { KEYWORD(DROP) }
"SKIP" { KEYWORD(SKIP) }
"LIST" { KEYWORD(LIST) }
"WITH" { KEYWORD(WITH) }
"LOAD" { KEYWORD(LOAD) }
"NEW" { KEYWORD(NEW) }
"ANY" { KEYWORD(ANY) }
"SET" { KEYWORD(SET) }
"ALL" { KEYWORD(ALL) }
"ASC" { KEYWORD(ASC) }
"NOT" { KEYWORD(NOT) }
"END" { KEYWORD(END) }
"XOR" { KEYWORD(XOR) }
"FOR" { KEYWORD(FOR) }
"ADD" { KEYWORD(ADD) }
"AND" { KEYWORD(AND) }
"OR" { KEYWORD(OR) }
"||" { KEYWORD(OR) }
"IN" { KEYWORD(IN) }
"IS" { KEYWORD(IS) }
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


'([^'\\\r\n]|\\.|\\)' {
    /* Character literal */
    yylval->emplace<char>(yytext[1]);
    return Token::CHAR_LITERAL;
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
    /* Float pattern */
    float v = 0.0f;
    std::from_chars(yytext, yytext + yyleng, v);
    yylval->emplace<float>(v);

    return Token::FLOAT;
}


-?[0-9]+[eE][+-]?[0-9]+[fd]? {
    /* Float pattern */
    float v = 0.0f;
    std::from_chars(yytext, yytext + yyleng, v);
    yylval->emplace<float>(v);

    return Token::FLOAT;
}


-?[0-9]+[fd] {
    float v = 0.0f;
    std::from_chars(yytext, yytext + yyleng, v);
    yylval->emplace<float>(v);

    return Token::FLOAT;
}


-?0[xX][0-9a-fA-F]+ {
    int64_t v = 0;
    std::from_chars(yytext, yytext + yyleng, v);
    yylval->emplace<int64_t>(v);

    return Token::DIGIT;
}


-?0[0-7]+ {
    int64_t v = 0;
    std::from_chars(yytext, yytext + yyleng, v);
    yylval->emplace<int64_t>(v);

    return Token::DIGIT;
}


-?[0-9]+ {
    int64_t v = 0;
    std::from_chars(yytext, yytext + yyleng, v);
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
