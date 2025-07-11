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

%token PROG_END 0

%token TAIL_TAIL
%token TIP_TAIL_TAIL
%token TAIL_TAIL_TIP
%token TIP_TAIL_BRACKET
%token BRACKET_TAIL_TIP
%token TAIL_BRACKET
%token BRACKET_TAIL
%token SEMI_COLON
%token ADD_ASSIGN
%token NOT_EQUAL
%token RANGE
%token LE
%token GE
%token OPAREN
%token CPAREN
%token OBRACE
%token CBRACE
%token OBRACK
%token CBRACK
%token ASSIGN
%token DOLLAR
%token COMMA
%token COLON
%token CARET
%token PIPE
%token PLUS
%token MULT
%token ESC
%token SUB
%token DOT
%token DIV
%token MOD
%token GT
%token LT

// Keywords
%token<std::string> DESCENDING
%token<std::string> CONSTRAINT
%token<std::string> MANDATORY
%token<std::string> ASCENDING
%token<std::string> OPTIONAL
%token<std::string> CONTAINS
%token<std::string> DISTINCT
%token<std::string> EXTRACT
%token<std::string> REQUIRE
%token<std::string> STARTS
%token<std::string> UNIQUE
%token<std::string> FILTER
%token<std::string> SINGLE
%token<std::string> SCALAR
%token<std::string> UNWIND
%token<std::string> REMOVE
%token<std::string> RETURN
%token<std::string> CREATE
%token<std::string> DELETE
%token<std::string> DETACH
%token<std::string> EXISTS
%token<std::string> IS_NOT
%token<std::string> LIMIT
%token<std::string> YIELD
%token<std::string> MATCH
%token<std::string> MERGE
%token<std::string> ORDER
%token<std::string> WHERE
%token<std::string> UNION
%token<std::string> FALSE
%token<std::string> COUNT
%token<std::string> DESC
%token<std::string> CALL
%token<std::string> NULL_
%token<std::string> TRUE
%token<std::string> WHEN
%token<std::string> NONE
%token<std::string> THEN
%token<std::string> ELSE
%token<std::string> CASE
%token<std::string> ENDS
%token<std::string> DROP
%token<std::string> SKIP
%token<std::string> WITH
%token<std::string> ANY
%token<std::string> SET
%token<std::string> ALL
%token<std::string> ASC
%token<std::string> NOT
%token<std::string> END
%token<std::string> XOR
%token<std::string> FOR
%token<std::string> ADD
%token<std::string> AND
%token<std::string> OR
%token<std::string> IN
%token<std::string> IS
%token<std::string> BY
%token<std::string> DO
%token<std::string> OF
%token<std::string> ON
%token<std::string> AS

%token<std::string> ESC_LITERAL
%token<std::string> STRING_LITERAL
%token<std::string> ID
%token<char> CHAR_LITERAL
%token<int64_t> DIGIT
%token<float> FLOAT

%token UNKNOWN

%type<std::string> symbol
%type<std::string> name
%type<std::string> qualifiedName
%type<std::string> invocationName
%type<std::string> reservedWord

%expect 0

%start script

%%

script
    : queries
    | queries SEMI_COLON
    ;

queries
    : query
    | queries SEMI_COLON query
    ;

query
    : singleQuery
    | singleQuery unionList
    ;

unionList
    : unionSt { scanner.notImplemented("Unions"); }
    | unionList unionSt
    ;

singleQuery
    : singlePartQ
    | multiPartQ
    ;

returnSt
    : RETURN projectionBody
    ;

withSt
    : WITH projectionBody where { scanner.notImplemented("WITH ... WHERE"); }
    | WITH projectionBody { scanner.notImplemented("WITH"); }
    ;

skipSt
    : SKIP expression { scanner.notImplemented("SKIP"); }
    ;

limitSt
    : LIMIT expression { scanner.notImplemented("LIMIT"); }
    ;

projectionBody
    : opt_Distinct projectionItems opt_orderSt opt_skipSt opt_limitSt
    ;

opt_Distinct
    : DISTINCT { scanner.notImplemented("DISTINCT"); }
    | /* empty */
    ;

opt_orderSt
    : orderSt
    | /* empty */
    ;

opt_skipSt
    : skipSt
    | /* empty */
    ;

opt_limitSt
    : limitSt
    | /* empty */
    ;

projectionItems
    : MULT
    | projectionItem
    | projectionItems COMMA projectionItem
    ;

projectionItem
    : expression
    | expression AS name
    ;

orderItem
    : expression
    | expression ASCENDING { scanner.notImplemented("ASCENDING"); }
    | expression ASC { scanner.notImplemented("ASC"); }
    | expression DESCENDING { scanner.notImplemented("DESCENDING"); }
    | expression DESC { scanner.notImplemented("DESC"); }
    ;

orderSt
    : ORDER BY orderItem { scanner.notImplemented("ORDER BY"); }
    | orderSt COMMA orderItem
    ;

singlePartQ
    : readingStatements returnSt
    | returnSt
    | readingStatements singlePartUpdateQ
    | singlePartUpdateQ
    ;

singlePartUpdateQ
    : updatingStatements
    | updatingStatements returnSt
    ;

readingStatements
    : readingStatement
    | readingStatements readingStatement
    ;

updatingStatements
    : updatingStatement
    | updatingStatements updatingStatement
    ;
 
multiPartQ
    : updateWithSt singlePartQ
    | readingStatements updateWithSt singlePartQ
    ;

updateWithSt
    : updatingStatements withSt
    | withSt
    | updateWithSt updatingStatements withSt
    | updateWithSt withSt
    ;

matchSt
    : MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt
    | OPTIONAL MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt { scanner.notImplemented("OPTIONAL MATCH"); }
    ;

unwindSt
    : UNWIND expression AS symbol { scanner.notImplemented("UNWIND"); }
    ;

readingStatement
    : matchSt
    | unwindSt
    | queryCallSt
    ;

updatingStatement
    : createSt
    | mergeSt
    | deleteSt
    | setSt
    | removeSt
    ;
 
deleteSt
    : DELETE expressionChain { scanner.notImplemented("DELETE"); }
    | DETACH DELETE expressionChain { scanner.notImplemented("DETACH DELETE"); }
    ;

removeSt
    : REMOVE removeItemChain { scanner.notImplemented("REMOVE"); }
    ;

removeItemChain
    : removeItem
    | removeItemChain COMMA removeItem
    ;

removeItem
    : symbol nodeLabels
    | propertyExpression
    ;

queryCallSt
    : CALL invocationName parenExpressionChain { scanner.notImplemented("CALL name(...)"); }
    | CALL invocationName parenExpressionChain yieldClause { scanner.notImplemented("CALL name(...) YIELD ..."); }
    | OPTIONAL CALL invocationName parenExpressionChain { scanner.notImplemented("OPTIONAL CALL name(...)"); }
    | OPTIONAL CALL invocationName parenExpressionChain yieldClause { scanner.notImplemented("OPTIONAL CALL name(...) YIELD ..."); }
    | CALL OBRACE query CBRACE { scanner.notImplemented("CALL { subquery }"); }
    | CALL OPAREN CPAREN OBRACE query CBRACE { scanner.notImplemented("CALL () { subquery }"); }
    | CALL OPAREN callCapture CPAREN OBRACE query CBRACE { scanner.notImplemented("CALL (..) { subquery }"); }
    ;

callCapture
    : symbol
    | symbol AS symbol
    | callCapture COMMA symbol
    | callCapture COMMA symbol AS symbol
    ;

expressionChain
    : expression
    | expressionChain COMMA expression
    ;

yieldClause
    : YIELD yieldItems
    | YIELD MULT
    ;

parenExpressionChain
    : OPAREN expressionChain CPAREN
    | OPAREN CPAREN
    ;

yieldItems
    : yieldItemChain
    | yieldItemChain where
    ;

yieldItemChain
    : yieldItem
    | yieldItemChain COMMA yieldItem
    ;

yieldItem
    : symbol
    | symbol AS symbol
    ;

mergeSt
    : MERGE patternPart { scanner.notImplemented("MERGE"); }
    | MERGE patternPart mergeActionChain { scanner.notImplemented("MERGE"); }
    ;

mergeActionChain
    : mergeAction
    | mergeActionChain mergeAction
    ;

mergeAction
    : ON MATCH setSt
    | ON CREATE setSt
    ;

setSt
    : SET setItem { scanner.notImplemented("SET"); }
    | setSt COMMA setItem
    ;

setItem
    : propertyExpression ASSIGN expression
    | symbol ADD_ASSIGN expression
    | symbol nodeLabels
    ;

nodeLabels
    : COLON name
    | COLON parameter
    | nodeLabels COLON name
    | nodeLabels COLON parameter
    ;

createSt
    : CREATE pattern { scanner.notImplemented("CREATE"); }
    ;

patternWhere
    : pattern
    | pattern where { scanner.notImplemented("WHERE"); }
    ;

where
    : WHERE expression { scanner.notImplemented("WHERE"); }
    ;

pattern
    : patternPart
    | pattern COMMA patternPart
    ;

expression
    : xorExpression
    | expression OR xorExpression { scanner.notImplemented("OR"); }
    ;

xorExpression
    : andExpression
    | xorExpression XOR andExpression { scanner.notImplemented("XOR"); }
    ;

andExpression
    : notExpression
    | andExpression AND notExpression { scanner.notImplemented("AND"); }
    ;

notExpression
    : comparisonExpression
    | NOT notExpression { scanner.notImplemented("NOT"); }
    ;

comparisonExpression
    : addSubExpression
    | comparisonExpression comparisonSigns addSubExpression { scanner.notImplemented("Comparison expressions"); }
    ;

comparisonSigns
    : ASSIGN
    | LE
    | GE
    | GT
    | LT
    | NOT_EQUAL
    | IS
    | IS_NOT
    ;

addSubExpression
    : multDivExpression
    | addSubExpression PLUS multDivExpression { scanner.notImplemented("Add/Sub expressions"); }
    | addSubExpression SUB multDivExpression { scanner.notImplemented("Add/Sub expressions"); }
    ;

multDivExpression
    : powerExpression
    | multDivExpression MULT powerExpression { scanner.notImplemented("Mult/Div expressions"); }
    | multDivExpression DIV powerExpression { scanner.notImplemented("Mult/Div expressions"); }
    | multDivExpression MOD powerExpression { scanner.notImplemented("Mult/Div expressions"); }
    ;

powerExpression
    : unaryAddSubExpression
    | powerExpression CARET unaryAddSubExpression { scanner.notImplemented("Power expressions"); }
    ;

unaryAddSubExpression
    : atomicExpression
    | PLUS atomicExpression { scanner.notImplemented("Unary Add/Sub expressions"); }
    | SUB atomicExpression { scanner.notImplemented("Unary Add/Sub expressions"); }
    ;

atomicExpression
    : propertyOrLabelExpression
    | atomicExpression stringExpression { scanner.notImplemented("String expressions"); }
    | atomicExpression listExpression { scanner.notImplemented("List expressions"); }
    ;

listExpression
    : IN propertyOrLabelExpression
    | OBRACK expression CBRACK
    | OBRACK expression RANGE expression CBRACK
    | OBRACK RANGE expression CBRACK
    | OBRACK expression RANGE CBRACK
    | OBRACK RANGE CBRACK
    ;

stringExpression
    : stringExpPrefix propertyOrLabelExpression
    ;

stringExpPrefix
    : STARTS WITH
    | ENDS WITH
    | CONTAINS
    ;

propertyOrLabelExpression
    : propertyExpression
    | propertyExpression nodeLabels
    ;

propertyExpression
    : atom
    | qualifiedName DOT name
    ;

patternPart
    : patternElem
    | symbol ASSIGN patternElem
    ;

patternElem
    : nodePattern
    | patternElem patternElemChain
    ;

patternElemChain
    : relationshipPattern nodePattern
    ;

properties
    : mapLit
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN
    ;

opt_symbol
    : symbol
    | /* empty */
    ;

opt_nodeLabels
    : nodeLabels
    | /* empty */
    ;

opt_properties
    : properties
    | /* empty */
    ;

opt_relationshipTypes
    : relationshipTypes
    | /* empty */
    ;

opt_rangeLit
    : rangeLit
    | /* empty */
    ;

atom
    : literal
    | parameter
    | caseExpression
    | countFunc
    | listComprehension
    | patternComprehension
    | filterWith
    //| relationshipsChainPattern // Enabling this causes conflicts, not sure if we need it
    | parenthesizedExpression
    | functionInvocation
    | symbol
    | subqueryExist
    ;

lhs
    : symbol ASSIGN
    ;


relationshipPattern
    : TAIL_TAIL     // --
    | TIP_TAIL_TAIL // <--
    | TAIL_TAIL_TIP // -->
    | TAIL_BRACKET relationDetail BRACKET_TAIL     // -[]-
    | TIP_TAIL_BRACKET relationDetail BRACKET_TAIL // <-[]-
    | TAIL_BRACKET relationDetail BRACKET_TAIL_TIP // -[]->
    ;

relationDetail
    : opt_symbol opt_relationshipTypes opt_rangeLit opt_properties
    ;

relationshipTypes
    : COLON name
    | relationshipTypes PIPE name
    | relationshipTypes PIPE COLON name
    ;

unionSt
    : UNION singleQuery { scanner.notImplemented("UNION"); }
    | UNION ALL singleQuery { scanner.notImplemented("UNION ALL"); }
    ;

subqueryExist
    : EXISTS OBRACE query CBRACE { scanner.notImplemented("EXISTS"); }
    | EXISTS OBRACE patternWhere CBRACE { scanner.notImplemented("EXISTS"); }
    ;

qualifiedName
    : symbol { $$ = std::move($1); }
    | qualifiedName DOT symbol { $$ = std::move($1); $$ += "." + std::move($3); }
    ;

invocationName
    : qualifiedName { $$ = std::move($1); }
    ;

functionInvocation
    : invocationName OPAREN CPAREN { scanner.notImplemented("Function invocations"); }
    | invocationName OPAREN DISTINCT CPAREN { scanner.notImplemented("Function invocations"); }
    | invocationName OPAREN expressionChain CPAREN { scanner.notImplemented("Function invocations"); }
    | invocationName OPAREN DISTINCT expressionChain CPAREN { scanner.notImplemented("Function invocations"); }
    ;

parenthesizedExpression
    : OPAREN expression CPAREN
    ;

filterWith
    : filterKeyword OPAREN filterExpression CPAREN { scanner.notImplemented("Filters"); }
    ;

filterKeyword
    : ALL
    | ANY
    | NONE
    | SINGLE
    ;

patternComprehension
    : OBRACK relationshipsChainPattern PIPE expression CBRACK
    | OBRACK lhs relationshipsChainPattern PIPE expression CBRACK
    | OBRACK relationshipsChainPattern where PIPE expression CBRACK
    | OBRACK lhs relationshipsChainPattern where PIPE expression CBRACK
    ;

relationshipsChainPattern
    : nodePattern patternElemChain
    | relationshipsChainPattern patternElemChain
    ;

listComprehension
    : OBRACK filterExpression CBRACK { scanner.notImplemented("List comprehensions"); }
    | OBRACK filterExpression PIPE expression CBRACK { scanner.notImplemented("List comprehensions"); }
    ;

filterExpression
    : symbol IN expression { scanner.notImplemented("IN"); }
    | symbol IN expression where { scanner.notImplemented("IN"); }
    ;

countFunc
    : COUNT OPAREN MULT CPAREN { scanner.notImplemented("COUNT"); }
    | COUNT OPAREN CPAREN { scanner.notImplemented("COUNT"); }
    | COUNT OPAREN DISTINCT CPAREN { scanner.notImplemented("COUNT"); }
    | COUNT OPAREN expressionChain CPAREN { scanner.notImplemented("COUNT"); }
    | COUNT OPAREN DISTINCT expressionChain CPAREN { scanner.notImplemented("COUNT"); }
    | COUNT OBRACE patternWhere CBRACE { scanner.notImplemented("COUNT"); }

    // Here, returnSt is mandatory for MATCH subqueries, as opposed to Neo4j's Cypher parser
    | COUNT OBRACE query CBRACE { scanner.notImplemented("COUNT"); }
    ;

caseExpression
    : CASE whenThenChain END { scanner.notImplemented("CASE"); }
    | CASE expression whenThenChain END  { scanner.notImplemented("CASE"); }
    | CASE whenThenChain ELSE expression END { scanner.notImplemented("CASE"); }
    | CASE expression whenThenChain ELSE expression END { scanner.notImplemented("CASE"); }
    ;

whenThenChain
    : whenThen
    | whenThenChain whenThen
    ;

whenThen
    : WHEN expression THEN expression
    ;

parameter
    : DOLLAR symbol { scanner.notImplemented("Parameters"); }
    | DOLLAR numLit { scanner.notImplemented("Parameters"); }
    ;

literal
    : boolLit
    | numLit
    | NULL_ { scanner.notImplemented("NULL"); }
    | stringLit
    | charLit
    | listLit { scanner.notImplemented("Lists"); }
    | mapLit { scanner.notImplemented("Maps"); }
    ;

rangeLit
    : MULT { scanner.notImplemented("Ranges"); }
    | MULT numLit { scanner.notImplemented("Ranges"); }
    | MULT RANGE { scanner.notImplemented("Ranges"); }
    | MULT RANGE numLit { scanner.notImplemented("Ranges"); }
    | MULT numLit RANGE { scanner.notImplemented("Ranges"); }
    | MULT numLit RANGE numLit { scanner.notImplemented("Ranges"); }
    ;

boolLit
    : TRUE
    | FALSE
    ;

numLit
    : DIGIT
    | FLOAT
    ;

stringLit
    : STRING_LITERAL
    ;

charLit
    : CHAR_LITERAL
    ;

listLit
    : OBRACK CBRACK { scanner.notImplemented("Lists"); }
    //| OBRACK expressionChain CBRACK // Enabling this causes conflicts
    // Instead using this: (simpler, too simple?)
    | OBRACK listLitItems CBRACK { scanner.notImplemented("Lists"); }
    ;

listLitItems
    : listLitItem
    | listLitItems COMMA listLitItem
    ;

listLitItem
    : literal
    | parameter { scanner.notImplemented("Parameters"); }
    | caseExpression { scanner.notImplemented("CASE"); }
    | countFunc { scanner.notImplemented("COUNT"); }
    | listComprehension { scanner.notImplemented("List comprehensions"); }
    | patternComprehension { scanner.notImplemented("Pattern comprehensions"); }
    | filterWith { scanner.notImplemented("Filters"); }
    //| parenthesizedExpression // Enabling this causes conflicts, not needed?
    | functionInvocation { scanner.notImplemented("Function invocations"); }
    | symbol
    | subqueryExist { scanner.notImplemented("EXISTS"); }
    ;

mapLit
    : OBRACE CBRACE { scanner.notImplemented("Maps"); }
    | OBRACE mapPairChain CBRACE { scanner.notImplemented("Maps"); }
    ;

mapPairChain
    : mapPair { scanner.notImplemented("Maps"); }
    | mapPairChain COMMA mapPair { scanner.notImplemented("Maps"); }
    ;

mapPair
    : name COLON expression { scanner.notImplemented("Maps"); }
    ;

name
    : symbol
    | reservedWord
    ;

symbol
    : ESC_LITERAL
    | ID
    //| FILTER // We should not need to support these
    //| EXTRACT
    //| ANY
    //| NONE
    //| SINGLE
    ;

reservedWord
    : ALL
    | ASC
    | ASCENDING
    | BY
    | CREATE
    | DELETE
    | DESC
    | DESCENDING
    | DETACH
    | EXISTS
    | LIMIT
    | MATCH
    | MERGE
    | ON
    | OPTIONAL
    | ORDER
    | REMOVE
    | RETURN
    | SET
    | SKIP
    | WHERE
    | WITH
    | UNION
    | UNWIND
    | AND
    | AS
    | CONTAINS
    | DISTINCT
    | ENDS
    | IN
    | IS
    | NOT
    | OR
    | STARTS
    | XOR
    | FALSE
    | TRUE
    | NULL_
    | CONSTRAINT
    | DO
    | FOR
    | REQUIRE
    | UNIQUE
    | CASE
    | WHEN
    | THEN
    | ELSE
    | END
    | MANDATORY
    | SCALAR
    | OF
    | ADD
    | DROP
    ;


%%

void db::YCypherParser::error(const location_type& l, const std::string& m) {
    location turingLoc = scanner.getLocation();
    scanner.syntaxError(m);
}


