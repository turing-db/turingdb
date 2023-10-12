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

"list"    { return YParser::make_LIST(); }
"databases" { return YParser::make_DATABASES(); }
"open" { return YParser::make_OPEN(); }
"close" { return YParser::make_CLOSE(); }
"select" { return YParser::make_SELECT(); }
"from" { return YParser::make_FROM(); }
"or" { return YParser::make_OR(); }
"and" { return YParser::make_AND(); }
"is" { return YParser::make_LIKE(); }

"," { return YParser::make_COMMA(); }
":" { return YParser::make_COLON(); }
"*" { return YParser::make_STAR(); }
"{" { return YParser::make_OBRACK(); }
"}" { return YParser::make_CBRACK(); }
"(" { return YParser::make_OPAR(); }
")" { return YParser::make_CPAR(); }
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

[a-zA-Z]+[a-zA-Z0-9_]* { return YParser::make_ID(std::string(yytext)); }

\"[^\"]*\" {
               if (yyleng <= 2) {
                   return YParser::make_STRING_CONSTANT("");
               }
               return YParser::make_STRING_CONSTANT(std::string(yytext+1, yyleng-2));
           } 

.           { return YParser::make_UNKNOWN_TOKEN(); }

<<EOF>>     {  yyterminate(); }

%%
