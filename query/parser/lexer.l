%{

#include "YScanner.h"
#include "parser.hpp"

using namespace db;

%}

%option noyywrap
%option c++
%option yyclass="YScanner"
%option prefix="YScanner_"

%%

[ \t\r\n]+  { }

"select"|"SELECT" { return YParser::make_SELECT(); }
"from"|"FROM" { return YParser::make_FROM(); }
"match"|"MATCH" { return YParser::make_MATCH(); }
"return"|"RETURN" { return YParser::make_RETURN(); }
"create"|"CREATE" { return YParser::make_CREATE(); }
"list"|"LIST" { return YParser::make_LIST(); }
"graph"|"GRAPH" { return YParser::make_GRAPH(); }
"load"|"LOAD" { return YParser::make_LOAD(); }
"explain"|"EXPLAIN" { return YParser::make_EXPLAIN(); }
"history"|"HISTORY" { return YParser::make_HISTORY(); }

"or"|"OR" { return YParser::make_OR(); }
"and"|"AND" { return YParser::make_AND(); }
"like"|"LIKE" { return YParser::make_LIKE(); }

"." { return YParser::make_POINT(); }
"," { return YParser::make_COMMA(); }
":" { return YParser::make_COLON(); }
"*" { return YParser::make_STAR(); }
"{" { return YParser::make_OBRACK(); }
"}" { return YParser::make_CBRACK(); }
"(" { return YParser::make_OPAR(); }
")" { return YParser::make_CPAR(); }
"->" { return YParser::make_ARROW(); }
"+" { return YParser::make_PLUS(); }
"-" { return YParser::make_MINUS(); }
">=" { return YParser::make_GREATER_EQUAL(); }
">" { return YParser::make_GREATER(); }
"<=" { return YParser::make_LOWER_EQUAL(); }
"<" { return YParser::make_LOWER(); }
"=" { return YParser::make_EQUAL(); }
"!=" { return YParser::make_NOT_EQUAL(); }
"!" { return YParser::make_NOT(); }
"[" { return YParser::make_OSBRACK(); }
"]" { return YParser::make_CSBRACK(); }

(True|False|TRUE|FALSE|true|false) { return YParser::make_BOOLEAN_CONSTANT(yytext); }

[a-zA-Z]+[a-zA-Z0-9_]* { return YParser::make_ID(std::string(yytext)); }

-?[0-9]+ { return YParser::make_INT_CONSTANT(yytext); }

-?[0-9]+("."[0-9]+)? { return YParser::make_DECIMAL_CONSTANT(yytext); }

\"[^\"]*\"|'[^\']*\' {
                           if (yyleng <= 2) {
                               return YParser::make_STRING_CONSTANT("");
                           }
                           return YParser::make_STRING_CONSTANT(std::string(yytext+1, yyleng-2));
                     } 

\`[^\"]*\` {
                           if (yyleng <= 2) {
                               return YParser::make_BACKTICK_STRING_CONSTANT("");
                           }
                           return YParser::make_BACKTICK_STRING_CONSTANT(std::string(yytext+1, yyleng-2));
           }

.           { return YParser::make_UNKNOWN_TOKEN(); }

%%
