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

\"[^\"]*\" {
               if (yyleng <= 2) {
                   return YParser::make_STRING_CONSTANT("");
               }
               return YParser::make_STRING_CONSTANT(std::string(yytext+1, yyleng-2));
           } 

.           { return YParser::make_UNKNOWN_TOKEN(); }

<<EOF>>     {  yyterminate(); }

%%
