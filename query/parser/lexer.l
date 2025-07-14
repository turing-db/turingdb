%{

#include "YScanner.h"
#include "parser.hpp"

using namespace db;

#define yyterminate() return YParser::make_YYEOF(location()); // otherwise loc panic on eof
#define YY_USER_ACTION advanceLocation(yyleng);  // each token, advance current location

%}

%option noyywrap
%option c++
%option yyclass="YScanner"
%option prefix="YScanner_"
%%

\n        { locationNewLine(); } // handle new lines specifically w.r.t location
[ \t\r]+  { }

%{
// Pass location when making each token for location based parse errors
%}
"select"|"SELECT" { return YParser::make_SELECT(getLocation()); }
"from"|"FROM" { return YParser::make_FROM(getLocation()); }
"match"|"MATCH" { return YParser::make_MATCH(getLocation()); }
"return"|"RETURN" { return YParser::make_RETURN(getLocation()); }
"create"|"CREATE" { return YParser::make_CREATE(getLocation()); }
"list"|"LIST" { return YParser::make_LIST(getLocation()); }
"graph"|"GRAPH" { return YParser::make_GRAPH(getLocation()); }
"load"|"LOAD" { return YParser::make_LOAD(getLocation()); }
"explain"|"EXPLAIN" { return YParser::make_EXPLAIN(getLocation()); }
"history"|"HISTORY" { return YParser::make_HISTORY(getLocation()); }
"change"|"CHANGE" { return YParser::make_CHANGE(getLocation()); }
"commit"|"COMMIT" { return YParser::make_COMMIT(getLocation()); }
"or"|"OR" { return YParser::make_OR(getLocation()); }
"and"|"AND" { return YParser::make_AND(getLocation()); }
"like"|"LIKE" { return YParser::make_LIKE(getLocation()); }
"new"|"NEW" { return YParser::make_NEW(getLocation()); }
"submit"|"SUBMIT" { return YParser::make_SUBMIT(getLocation()); }
"delete"|"DELETE" { return YParser::make_DELETE(getLocation()); }
"call"|"CALL" { return YParser::make_CALL(getLocation()); }
"@"|"at"|"AT" { return YParser::make_AT(getLocation()); }

"properties"|"PROPERTIES" { return YParser::make_PROPERTIES(getLocation()); }
"labels"|"LABELS" { return YParser::make_LABELS(getLocation()); }
"edgetypes"|"EDGETYPES" { return YParser::make_EDGETYPES(getLocation()); }
"labelsets"|"LABELSETS" { return YParser::make_LABELSETS(getLocation()); }

"." { return YParser::make_POINT(getLocation()); }
"," { return YParser::make_COMMA(getLocation()); }
":" { return YParser::make_COLON(getLocation()); }
"*" { return YParser::make_STAR(getLocation()); }
"{" { return YParser::make_OBRACK(getLocation()); }
"}" { return YParser::make_CBRACK(getLocation()); }
"(" { return YParser::make_OPAR(getLocation()); }
")" { return YParser::make_CPAR(getLocation()); }
"->" { return YParser::make_ARROW(getLocation()); }
"+" { return YParser::make_PLUS(getLocation()); }
"-" { return YParser::make_MINUS(getLocation()); }
">=" { return YParser::make_GREATER_EQUAL(getLocation()); }
">" { return YParser::make_GREATER(getLocation()); }
"<=" { return YParser::make_LOWER_EQUAL(getLocation()); }
"<" { return YParser::make_LOWER(getLocation()); }
"=" { return YParser::make_EQUAL(getLocation()); }
"!=" { return YParser::make_NOT_EQUAL(getLocation()); }
"~=" { return YParser::make_STR_APPROX(getLocation()); }
"!" { return YParser::make_NOT(getLocation()); }
"[" { return YParser::make_OSBRACK(getLocation()); }
"]" { return YParser::make_CSBRACK(getLocation()); }

"u" { return YParser::make_UNSIGNED_SUFFIX(getLocation()); }

(True|False|TRUE|FALSE|true|false) { return YParser::make_BOOLEAN_CONSTANT(yytext, getLocation()); }

[a-zA-Z]+[a-zA-Z0-9_]* { return YParser::make_ID(std::string(yytext), getLocation()); }

[0-9]+ { return YParser::make_INT_CONSTANT(yytext, getLocation()); }

-?[0-9]+"."([0-9]+)? { return YParser::make_DECIMAL_CONSTANT(yytext, getLocation()); }

\"[^\"]*\"|'[^\']*\' {
                           if (yyleng <= 2) {
                               return YParser::make_STRING_CONSTANT("", getLocation());
                           }
                           return YParser::make_STRING_CONSTANT(std::string(yytext+1, yyleng-2), getLocation());
                     } 

\`[^\"]*\` {
                           if (yyleng <= 2) {
                               return YParser::make_BACKTICK_STRING_CONSTANT("", getLocation());
                           }
                           return YParser::make_BACKTICK_STRING_CONSTANT(std::string(yytext+1, yyleng-2), getLocation());
           }

.           { return YParser::make_UNKNOWN_TOKEN(getLocation()); }

%%
