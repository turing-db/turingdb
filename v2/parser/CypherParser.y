%require "3.2"
%language "c++"

%define api.parser.class { YCypherParser }
%define api.value.type variant
%define parse.assert
%define parse.error detailed
%define api.namespace { db }
%parse-param {db::YCypherScanner& scanner}
%parse-param {db::CypherAST& ast}
%locations

%header
%verbose

%code requires {
    #include <spdlog/fmt/bundled/core.h>
    #include <optional>
    #include <vector>

    #include "expressions/Operators.h"
    #include "Literal.h"
    #include "Atom.h"
    #include "Symbol.h"
    #include "QualifiedName.h"

    namespace db {
        class YCypherScanner;
        class CypherAST;
    }
}

%code top {
    #include "YCypherScanner.h"
    #include "GeneratedCypherParser.h"
    #include "CypherAST.h"

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

%type<db::Literal> numLit boolLit charLit literal stringLit
%type<db::Atom> atom
%type<db::Symbol> symbol
%type<db::QualifiedName> qualifiedName
%type<db::QualifiedName> invocationName
%type<std::string> name
%type<std::string> reservedWord

%type<std::vector<std::string>> nodeLabels
%type<std::vector<std::string>> relationshipTypes

%type<std::optional<Symbol>> opt_symbol
%type<std::optional<std::vector<std::string>>> opt_nodeLabels
%type<std::optional<std::vector<std::string>>> opt_relationshipTypes

%type<BinaryOperator> comparisonSign
%type<StringOperator> stringExpPrefix

%expect 0

%start script

%%

script
    : queries
    | queries SEMI_COLON
    ;

queries
    : query { fmt::print("Finished query\n"); }
    | queries SEMI_COLON query { scanner.notImplemented("Multiple queries"); }
    ;

query
    : singleQuery
    | singleQuery unionList { scanner.notImplemented("Query + Unions"); }
    ;

unionList
    : unionSt { scanner.notImplemented("Unions"); }
    | unionList unionSt
    ;

singleQuery
    : singlePartQ
    | multiPartQ { scanner.notImplemented("Multi-part queries"); }
    ;

returnSt
    : RETURN projectionBody { fmt::print("RETURN projectionBody\n"); }
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
    : MULT { fmt::print("projectionItems '*'\n"); }
    | projectionItem { fmt::print("projectionItem\n"); }
    | projectionItems COMMA projectionItem
    ;

projectionItem
    : expression { fmt::print("projectionItem\n"); }
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
    | returnSt { scanner.notImplemented("Return only"); }
    | readingStatements singlePartUpdateQ { scanner.notImplemented("Reading statement + Update"); }
    | singlePartUpdateQ { scanner.notImplemented("Single-part update"); }
    ;

singlePartUpdateQ
    : updatingStatements { scanner.notImplemented("Single-part update"); }
    | updatingStatements returnSt { scanner.notImplemented("Single-part update + Return"); }
    ;

readingStatements
    : readingStatement
    | readingStatements readingStatement {}
    ;

updatingStatements
    : updatingStatement { scanner.notImplemented("Updating statement"); }
    | updatingStatements updatingStatement { scanner.notImplemented("Multiple updating statements"); }
    ;
 
multiPartQ
    : updateWithSt singlePartQ { scanner.notImplemented("Update + With + Reading"); }
    | readingStatements updateWithSt singlePartQ { scanner.notImplemented("Reading + With + Update + Reading"); }
    ;

updateWithSt
    : updatingStatements withSt { scanner.notImplemented("Update + With"); }
    | withSt { scanner.notImplemented("With"); }
    | updateWithSt updatingStatements withSt { scanner.notImplemented("Update + With + Update"); }
    | updateWithSt withSt { scanner.notImplemented("Update + With"); }
    ;

matchSt
    : MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt { fmt::print("MATCH patternWhere\n"); }
    | OPTIONAL MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt { scanner.notImplemented("OPTIONAL MATCH"); }
    ;

unwindSt
    : UNWIND expression AS symbol { scanner.notImplemented("UNWIND"); }
    ;

readingStatement
    : matchSt { /* ast.finishMatch(); */  }
    | unwindSt { scanner.notImplemented("UNWIND"); }
    | queryCallSt { scanner.notImplemented("CALL"); }
    ;

updatingStatement
    : createSt { scanner.notImplemented("CREATE"); }
    | mergeSt { scanner.notImplemented("MERGE"); }
    | deleteSt { scanner.notImplemented("DELETE"); }
    | setSt { scanner.notImplemented("SET"); }
    | removeSt { scanner.notImplemented("REMOVE"); }
    ;
 
deleteSt
    : DELETE expressionChain { scanner.notImplemented("DELETE"); }
    | DETACH DELETE expressionChain { scanner.notImplemented("DETACH DELETE"); }
    ;

removeSt
    : REMOVE removeItemChain { scanner.notImplemented("REMOVE"); }
    ;

removeItemChain
    : removeItem { scanner.notImplemented("REMOVE"); }
    | removeItemChain COMMA removeItem { scanner.notImplemented("REMOVE multiple items"); }
    ;

removeItem
    : symbol nodeLabels { scanner.notImplemented("REMOVE"); }
    | propertyExpression { scanner.notImplemented("REMOVE"); }
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
    : symbol { scanner.notImplemented("CALL capture"); }
    | symbol AS symbol { scanner.notImplemented("CALL capture"); }
    | callCapture COMMA symbol { scanner.notImplemented("CALL capture"); }
    | callCapture COMMA symbol AS symbol { scanner.notImplemented("CALL capture"); }
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
    : COLON name { $$ = {std::move($2)}; }
    | COLON parameter { scanner.notImplemented("Parameters"); }
    | nodeLabels COLON name { $$.emplace_back(std::move($3)); }
    | nodeLabels COLON parameter { scanner.notImplemented("Parameters"); }
    ;

createSt
    : CREATE pattern { scanner.notImplemented("CREATE"); }
    ;

patternWhere
    : pattern { fmt::print("pattern\n"); }
    | pattern where { }
    ;

where
    : WHERE expression { }
    ;

pattern
    : patternPart { fmt::print("patternPart\n"); }
    | pattern COMMA patternPart { fmt::print("pattern, patternPart\n"); }
    ;

expression
    : xorExpression { fmt::print("expression\n"); }
    | expression OR xorExpression { fmt::print("expression OR xorExpression\n"); }
    ;

xorExpression
    : andExpression { fmt::print("xorExpression\n"); }
    | xorExpression XOR andExpression  { fmt::print("xorExpression XOR andExpression\n"); }
    ;

andExpression
    : notExpression { fmt::print("andExpression\n"); }
    | andExpression AND notExpression { fmt::print("andExpression AND notExpression\n"); }
    ;

notExpression
    : comparisonExpression { fmt::print("notExpression\n"); }
    | NOT notExpression { fmt::print("NOT notExpression\n"); }
    ;

comparisonExpression
    : addSubExpression { fmt::print("comparisonExpression\n"); }
    | comparisonExpression comparisonSign addSubExpression { fmt::print("comparisonExpression comparisonSign addSubExpression\n"); }
    ;

comparisonSign
    : ASSIGN { $$ = BinaryOperator::Equal; }
    | LE { $$ = BinaryOperator::LessThanOrEqual; }
    | GE { $$ = BinaryOperator::GreaterThanOrEqual; }
    | GT { $$ = BinaryOperator::GreaterThan; }
    | LT { $$ = BinaryOperator::LessThan; }
    | NOT_EQUAL { $$ = BinaryOperator::NotEqual; }
    | IS { $$ = BinaryOperator::Equal; }
    | IS_NOT { $$ = BinaryOperator::NotEqual; }
    ;

addSubExpression
    : multDivExpression { fmt::print("addSubExpression\n"); }
    | addSubExpression PLUS multDivExpression { fmt::print("addSubExpression PLUS multDivExpression\n"); }
    | addSubExpression SUB multDivExpression { fmt::print("addSubExpression SUB multDivExpression\n"); }
    ;

multDivExpression
    : powerExpression { fmt::print("multDivExpression\n"); }
    | multDivExpression MULT powerExpression { fmt::print("multDivExpression MULT powerExpression\n"); }
    | multDivExpression DIV powerExpression { fmt::print("multDivExpression DIV powerExpression\n"); }
    | multDivExpression MOD powerExpression { fmt::print("multDivExpression MOD powerExpression\n"); }
    ;

powerExpression
    : unaryAddSubExpression { fmt::print("powerExpression\n"); }
    | powerExpression CARET unaryAddSubExpression { fmt::print("powerExpression CARET unaryAddSubExpression\n"); }
    ;

unaryAddSubExpression
    : atomicExpression { fmt::print("unaryAddSubExpression\n"); }
    | PLUS atomicExpression { scanner.notImplemented("Unary Add/Sub expressions"); }
    | SUB atomicExpression { scanner.notImplemented("Unary Add/Sub expressions"); }
    ;

atomicExpression
    : propertyOrLabelExpression { fmt::print("atomicExpression\n"); }
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
    : stringExpPrefix propertyOrLabelExpression { fmt::print("stringExpPrefix propertyOrLabelExpression\n"); }
    ;

stringExpPrefix
    : STARTS WITH { $$ = StringOperator::StartsWith; }
    | ENDS WITH { $$ = StringOperator::EndsWith; }
    | CONTAINS { $$ = StringOperator::Contains; }
    ;

propertyOrLabelExpression
    : propertyExpression { fmt::print("propertyExpression\n"); }
    | propertyExpression nodeLabels { fmt::print("propertyExpression nodeLabels\n"); }
    ;

propertyExpression
    : atom { fmt::print("atom\n"); }
    | qualifiedName DOT name { fmt::print("qualifiedName DOT name\n"); }
    ;

patternPart
    : patternElem
    | patternAlias { scanner.notImplemented("Pattern alias: Symbol = ()-[]-()-[]-()..."); }
    ;

patternAlias
    : symbol ASSIGN patternElem { scanner.notImplemented("Pattern alias: Symbol = ()-[]-()-[]-()..."); }

patternElem
    : nodePattern { fmt::print("patternElem: Node\n"); }
    | patternElem patternElemChain
    ;

patternElemChain
    : relationshipPattern nodePattern { fmt::print("patternElemChain: Edge + Node\n"); }
    ;

properties
    : mapLit
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN { ast.newNodePattern(std::move($2), std::move($3)); }
    ;

opt_symbol
    : symbol { $$ = std::move($1); }
    | { $$ = std::nullopt; }
    ;

opt_nodeLabels
    : nodeLabels { $$ = std::move($1); }
    | { $$ = std::nullopt; }
    ;

opt_properties
    : properties { scanner.notImplemented("Properties"); }
    | /* empty */
    ;

opt_relationshipTypes
    : relationshipTypes { $$ = std::move($1); }
    | { $$ = std::nullopt; }
    ;

opt_rangeLit
    : rangeLit
    | /* empty */
    ;

atom
    : literal { $$ = std::move($1); }
    | parameter { scanner.notImplemented("Parameters"); }
    | caseExpression { scanner.notImplemented("CASE"); }
    | countFunc { scanner.notImplemented("COUNT"); }
    | listComprehension { scanner.notImplemented("List comprehensions"); }
    | patternComprehension { scanner.notImplemented("Pattern comprehensions"); }
    | filterWith { scanner.notImplemented("Filter keywords"); }
    //| relationshipsChainPattern // Enabling this causes conflicts, not sure if we need it
    | parenthesizedExpression { scanner.notImplemented("Parenthesized expressions"); }
    | functionInvocation { scanner.notImplemented("Function invocations"); }
    | symbol { $$ = std::move($1); }
    | subqueryExist { scanner.notImplemented("EXISTS"); }
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
    : COLON name { $$ = {std::move($2)}; }
    | relationshipTypes PIPE name { scanner.notImplemented("EdgeType | EdgeType | ..."); }
    | relationshipTypes PIPE COLON name { scanner.notImplemented("EdgeType | EdgeType | ..."); }
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
    : symbol { $$.addSymbol(std::move($1)); }
    | qualifiedName DOT symbol { $$.addSymbol(std::move($3)); }
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
    : boolLit { $$ = Literal(std::move($1)); }
    | numLit { $$ = Literal(std::move($1)); }
    | NULL_ { $$ = Literal(std::nullopt); }
    | stringLit { $$ = Literal(std::move($1)); }
    | charLit { $$ = Literal(std::move($1)); }
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
    : TRUE { $$ = Literal(true); }
    | FALSE { $$ = Literal(false); }
    ;

numLit
    : DIGIT { $$ = Literal($1); }
    | FLOAT { $$ = Literal($1); }
    ;

stringLit
    : STRING_LITERAL { $$ = Literal(std::move($1)); }
    ;

charLit
    : CHAR_LITERAL { $$ = Literal($1); }
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
    : symbol { $$ = std::move($1._name) ; }
    | reservedWord { $$ = std::move($1) ; }
    ;

symbol
    : ESC_LITERAL { $$ = Symbol { ._name = std::move($1)}; }
    | ID { $$ = Symbol { ._name = std::move($1) }; }
    //| FILTER // We should not need to support these
    //| EXTRACT
    //| ANY
    //| NONE
    //| SINGLE
    ;

reservedWord
    : ALL { $$ = std::move($1); }
    | ASC { $$ = std::move($1); }
    | ASCENDING { $$ = std::move($1); }
    | BY { $$ = std::move($1); }
    | CREATE { $$ = std::move($1); }
    | DELETE { $$ = std::move($1); }
    | DESC { $$ = std::move($1); }
    | DESCENDING { $$ = std::move($1); }
    | DETACH { $$ = std::move($1); }
    | EXISTS { $$ = std::move($1); }
    | LIMIT { $$ = std::move($1); }
    | MATCH { $$ = std::move($1); }
    | MERGE { $$ = std::move($1); }
    | ON { $$ = std::move($1); }
    | OPTIONAL { $$ = std::move($1); }
    | ORDER { $$ = std::move($1); }
    | REMOVE { $$ = std::move($1); }
    | RETURN { $$ = std::move($1); }
    | SET { $$ = std::move($1); }
    | SKIP { $$ = std::move($1); }
    | WHERE { $$ = std::move($1); }
    | WITH { $$ = std::move($1); }
    | UNION { $$ = std::move($1); }
    | UNWIND { $$ = std::move($1); }
    | AND { $$ = std::move($1); }
    | AS { $$ = std::move($1); }
    | CONTAINS { $$ = std::move($1); }
    | DISTINCT { $$ = std::move($1); }
    | ENDS { $$ = std::move($1); }
    | IN { $$ = std::move($1); }
    | IS { $$ = std::move($1); }
    | NOT { $$ = std::move($1); }
    | OR { $$ = std::move($1); }
    | STARTS { $$ = std::move($1); }
    | XOR { $$ = std::move($1); }
    | FALSE { $$ = std::move($1); }
    | TRUE { $$ = std::move($1); }
    | NULL_ { $$ = std::move($1); }
    | CONSTRAINT { $$ = std::move($1); }
    | DO { $$ = std::move($1); }
    | FOR { $$ = std::move($1); }
    | REQUIRE { $$ = std::move($1); }
    | UNIQUE { $$ = std::move($1); }
    | CASE { $$ = std::move($1); }
    | WHEN { $$ = std::move($1); }
    | THEN { $$ = std::move($1); }
    | ELSE { $$ = std::move($1); }
    | END { $$ = std::move($1); }
    | MANDATORY { $$ = std::move($1); }
    | SCALAR { $$ = std::move($1); }
    | OF { $$ = std::move($1); }
    | ADD { $$ = std::move($1); }
    | DROP { $$ = std::move($1); }
    ;


%%

void db::YCypherParser::error(const location_type& l, const std::string& m) {
    location turingLoc = scanner.getLocation();
    scanner.syntaxError(m);
}


