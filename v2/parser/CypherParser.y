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

%left OR
%left XOR
%left AND
%left NOT
%left ASSIGN LE GE GT LT NOT_EQUAL CONTAINS IS IS_NOT
%left PLUS SUB
%left MULT DIV MOD
%left CARET
%left OBRACK

%right UPLUS USUB // Right associative for unary operators

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
    : unionSt
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
    : opt_Distinct projectionItems opt_orderSt opt_skipSt opt_limitSt
    ;

opt_Distinct
    : DISTINCT
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
    : MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt
    | OPTIONAL MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt
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
    | CALL invocationName parenExpressionChain yieldClause
    | OPTIONAL CALL invocationName parenExpressionChain
    | OPTIONAL CALL invocationName parenExpressionChain yieldClause
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
    : atomicExpression
    | expression OR expression
    | expression XOR expression
    | expression AND expression
    | expression NOT expression
    | expression ASSIGN expression
    | expression LE expression
    | expression GE expression
    | expression GT expression
    | expression LT expression
    | expression CONTAINS expression
    | expression IS expression
    | expression IS_NOT expression
    | expression NOT_EQUAL expression
    | expression PLUS expression
    | expression SUB expression
    | expression MULT expression
    | expression DIV expression
    | expression MOD expression
    | expression CARET expression
    | PLUS expression %prec UPLUS
    | SUB expression %prec USUB
    | STARTS WITH propertyOrLabelExpression
    | ENDS WITH propertyOrLabelExpression
    | CONTAINS propertyOrLabelExpression
    | IS_NOT NULL_
    | IS NULL_
    ;

atomicExpression
    : propertyOrLabelExpression
    | atomicExpression listExpression
    ;

listExpression
    : IN propertyOrLabelExpression
    | OBRACK expression CBRACK
    | OBRACK expression RANGE expression CBRACK
    | OBRACK RANGE expression CBRACK
    | OBRACK expression RANGE CBRACK
    | OBRACK RANGE CBRACK
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
    : UNION singleQuery
    | UNION ALL singleQuery
    ;

subqueryExist
    : EXISTS OBRACE query CBRACE
    | EXISTS OBRACE patternWhere CBRACE
    ;

qualifiedName
    : symbol { $$ = std::move($1); }
    | qualifiedName DOT symbol { $$ = std::move($1); $$ += "." + std::move($3); }
    ;

invocationName
    : qualifiedName { $$ = std::move($1); }
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

countFunc
    : COUNT OPAREN MULT CPAREN
    | COUNT OPAREN CPAREN
    | COUNT OPAREN DISTINCT CPAREN
    | COUNT OPAREN expressionChain CPAREN
    | COUNT OPAREN DISTINCT expressionChain CPAREN
    | COUNT OBRACE patternWhere CBRACE
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
    | countFunc
    | listComprehension
    | patternComprehension
    | filterWith
    //| parenthesizedExpression // Enabling this causes conflicts, not needed?
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


