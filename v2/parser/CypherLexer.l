%{

#include "YCypherScanner.h"
#include "GeneratedCypherParser.h"

using namespace db;

#define yyterminate() return db::YCypherParser::token::END_TOKEN
#define YY_USER_ACTION advanceLocation(yyleng);

%}

%option caseless noyywrap c++ yyclass="YCypherScanner"

%%

\n        { locationNewLine(); }
[ \t\r]+  { }

// General purpose tokens
"=" { return db::YCypherParser::token::ASSIGN; }
"+=" { return db::YCypherParser::token::ADD_ASSIGN; }
"<=" { return db::YCypherParser::token::LE; }
">=" { return db::YCypherParser::token::GE; }
">" { return db::YCypherParser::token::GT; }
"<" { return db::YCypherParser::token::LT; }
"<>" { return db::YCypherParser::token::NOT_EQUAL; }
"!=" { return db::YCypherParser::token::NOT_EQUAL; }
"->" { return db::YCypherParser::token::ARROW; }
".." { return db::YCypherParser::token::RANGE; }
";" { return db::YCypherParser::token::SEMI; }
"." { return db::YCypherParser::token::DOT; }
"," { return db::YCypherParser::token::COMMA; }
"(" { return db::YCypherParser::token::LPAREN; }
")" { return db::YCypherParser::token::RPAREN; }
"{" { return db::YCypherParser::token::LBRACE; }
"}" { return db::YCypherParser::token::RBRACE; }
"[" { return db::YCypherParser::token::LBRACK; }
"]" { return db::YCypherParser::token::RBRACK; }
"-" { return db::YCypherParser::token::SUB; }
"+" { return db::YCypherParser::token::PLUS; }
"/" { return db::YCypherParser::token::DIV; }
"%" { return db::YCypherParser::token::MOD; }
"^" { return db::YCypherParser::token::CARET; }
"*" { return db::YCypherParser::token::MULT; }
"`" { return db::YCypherParser::token::ESC; }
":" { return db::YCypherParser::token::COLON; }
"|" { return db::YCypherParser::token::PIPE; }
"$" { return db::YCypherParser::token::DOLLAR; }


.        { return YCypherParser::token::UNKNOWN; }

%%
