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

%token DESCENDING
%token CONSTRAINT
%token MANDATORY
%token ASCENDING
%token OPTIONAL
%token CONTAINS
%token DISTINCT
%token EXTRACT
%token REQUIRE
%token STARTS
%token UNIQUE
%token FILTER
%token SINGLE
%token SCALAR
%token UNWIND
%token REMOVE
%token RETURN
%token CREATE
%token DELETE
%token DETACH
%token EXISTS
%token LIMIT
%token YIELD
%token MATCH
%token MERGE
%token ORDER
%token WHERE
%token UNION
%token FALSE
%token COUNT
%token DESC
%token CALL
%token NULL_
%token TRUE
%token WHEN
%token NONE
%token THEN
%token ELSE
%token CASE
%token ENDS
%token DROP
%token SKIP
%token WITH
%token ANY
%token SET
%token ALL
%token ASC
%token NOT
%token END
%token XOR
%token FOR
%token ADD
%token AND
%token OR
%token IN
%token IS
%token BY
%token DO
%token OF
%token ON
%token AS

%token<std::string> ESC_LITERAL
%token<std::string> STRING_LITERAL
%token<std::string> ID
%token<char> CHAR_LITERAL
%token<int64_t> DIGIT
%token<float> FLOAT

%token UNKNOWN

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
    : regularQuery
    | standaloneCall
    ;

regularQuery
    : singleQuery
    | singleQuery unionList
    ;

unionList
    : unionSt
    | unionList unionSt
    ;

singleQuery
    : singlePartQ
    | multiPartQ
    ;

standaloneCall
    : CALL invocationName
    | CALL invocationName parenExpressionChain
    | CALL invocationName yieldClause
    | CALL invocationName parenExpressionChain yieldClause
    ;

returnSt
    : RETURN projectionBody
    ;

withSt
    : WITH projectionBody where
    | WITH projectionBody
    ;

skipSt
    : SKIP expression
    ;

limitSt
    : LIMIT expression
    ;

projectionBody
    : opt_Distinct projectionItems opt_OrderSt opt_skipSt opt_limitSt
    ;

opt_Distinct
    : DISTINCT
    | /* empty */
    ;

opt_OrderSt
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
    | expression AS symbol
    ;

orderItem
    : expression
    | expression ASCENDING
    | expression ASC
    | expression DESCENDING
    | expression DESC
    ;

orderSt
    : ORDER BY orderItem
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
    : MATCH patternWhere
    | OPTIONAL MATCH patternWhere
    ;

unwindSt
    : UNWIND expression AS symbol
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
    : DELETE expressionChain
    | DETACH DELETE expressionChain
    ;

removeSt
    : REMOVE removeItemChain
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
    : CALL invocationName parenExpressionChain
    | CALL invocationName parenExpressionChain YIELD yieldItems
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
    : MERGE patternPart
    | MERGE patternPart mergeActionChain
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
    : SET setItem
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
    : CREATE pattern
    ;

patternWhere
    : pattern
    | pattern where
    ;

where
    : WHERE expression
    ;

pattern
    : patternPart
    | pattern COMMA patternPart
    ;

expression
    : xorExpression
    | expression OR xorExpression
    ;

xorExpression
    : andExpression
    | xorExpression XOR andExpression
    ;

andExpression
    : notExpression
    | andExpression AND notExpression
    ;

notExpression
    : comparisonExpression
    | NOT notExpression
    ;

comparisonExpression
    : addSubExpression
    | comparisonExpression comparisonSigns addSubExpression
    ;

comparisonSigns
    : ASSIGN
    | LE
    | GE
    | GT
    | LT
    | NOT_EQUAL
    ;

addSubExpression
    : multDivExpression
    | addSubExpression PLUS multDivExpression
    | addSubExpression SUB multDivExpression
    ;

multDivExpression
    : powerExpression
    | multDivExpression MULT powerExpression
    | multDivExpression DIV powerExpression
    | multDivExpression MOD powerExpression
    ;

powerExpression
    : unaryAddSubExpression
    | powerExpression CARET unaryAddSubExpression
    ;

unaryAddSubExpression
    : atomicExpression
    | PLUS atomicExpression
    | SUB atomicExpression
    ;

atomicExpression
    : propertyOrLabelExpression
    | atomicExpression stringExpression
    | atomicExpression listExpression
    | atomicExpression nullExpression
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

nullExpression
    : IS NOT NULL_
    | IS NULL_
    ;

propertyOrLabelExpression
    : propertyExpression
    | propertyExpression nodeLabels
    ;

propertyExpression
    : atom
    | propertyExpression DOT name
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
    | parameter
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
    | countAll
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
    : UNION singleQuery
    | UNION ALL singleQuery
    ;

subqueryExist
    : EXISTS OBRACE regularQuery CBRACE
    | EXISTS OBRACE patternWhere CBRACE
    ;

invocationName
    : symbol
    //| invocationName DOT symbol // Enabling this causes conflicts. We need this!
    ;

functionInvocation
    : invocationName OPAREN CPAREN
    | invocationName OPAREN DISTINCT CPAREN
    | invocationName OPAREN expressionChain CPAREN
    | invocationName OPAREN DISTINCT expressionChain CPAREN
    ;

parenthesizedExpression
    : OPAREN expression CPAREN
    ;

filterWith
    : filterKeyword OPAREN filterExpression CPAREN
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
    : OBRACK filterExpression CBRACK
    | OBRACK filterExpression PIPE expression CBRACK
    ;

filterExpression
    : symbol IN expression
    | symbol IN expression where
    ;

countAll
    : COUNT OPAREN MULT CPAREN
    ;

caseExpression
    : CASE whenThenChain END
    | CASE expression whenThenChain END
    | CASE whenThenChain ELSE expression END
    | CASE expression whenThenChain ELSE expression END
    ;

whenThenChain
    : whenThen
    | whenThenChain whenThen
    ;

whenThen
    : WHEN expression THEN expression
    ;

parameter
    : DOLLAR symbol
    | DOLLAR numLit
    ;

literal
    : boolLit
    | numLit
    | NULL_
    | stringLit
    | charLit
    | listLit
    | mapLit
    ;

rangeLit
    : MULT
    | MULT numLit
    | MULT RANGE
    | MULT RANGE numLit
    | MULT numLit RANGE
    | MULT numLit RANGE numLit
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
    : OBRACK CBRACK
    //| OBRACK expressionChain CBRACK // Enabling this causes conflicts
    | OBRACK listLitItems CBRACK // Instead using this (simpler, too simple?)
    ;

listLitItems
    : listLitItem
    | listLitItems COMMA listLitItem
    ;

listLitItem
    : literal
    | parameter
    | caseExpression
    | countAll
    | listComprehension
    | patternComprehension
    | filterWith
    //| parenthesizedExpression // Enabling this causes conflicts
    | functionInvocation
    | symbol
    | subqueryExist
    ;

mapLit
    : OBRACE CBRACE
    | OBRACE mapPairChain CBRACE
    ;

mapPairChain
    : mapPair
    | mapPairChain COMMA mapPair
    ;

mapPair
    : name COLON expression
    ;

name
    : symbol
    | reservedWord
    ;

symbol
    : ESC_LITERAL
    | ID
    //| COUNT // Not sure needing this yet
    //| FILTER
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


