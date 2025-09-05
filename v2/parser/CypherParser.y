%require "3.2"
%language "c++"

%define api.parser.class { YCypherParser }
%define api.value.type variant
%define parse.assert
%define parse.error detailed
%define api.namespace { db::v2 }
%parse-param {db::v2::YCypherScanner& scanner}
%parse-param {db::v2::CypherAST& ast}
%define api.location.type { SourceLocation }
%locations

%header
%verbose

%code requires {
    // Inspired by https://github.com/antlr/grammars-v4/blob/master/cypher/CypherParser.g4

    #define LOC(obj, loc) ast.setLocation(obj, loc);

    #include <spdlog/fmt/bundled/core.h>
    #include <optional>
    #include <vector>

    #include "SourceLocation.h"

    #include "statements/StatementContainer.h"
    #include "statements/Return.h"
    #include "statements/Match.h"
    #include "expressions/All.h"
    #include "types/Literal.h"
    #include "types/Symbol.h"
    #include "types/WhereClause.h"
    #include "types/Pattern.h"
    #include "types/NodePattern.h"
    #include "types/EdgePattern.h"
    #include "types/SinglePartQuery.h"
    #include "types/Projection.h"

    namespace db::v2 {
        class YCypherScanner;
        class CypherAST;
        class QualifiedName;
    }
}

%code top {
    #include "YCypherScanner.h"
    #include "GeneratedCypherParser.h"
    #include "CypherAST.h"
    #include "types/QualifiedName.h"

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
%token<std::string_view> DESCENDING
%token<std::string_view> CONSTRAINT
%token<std::string_view> MANDATORY
%token<std::string_view> ASCENDING
%token<std::string_view> OPTIONAL
%token<std::string_view> CONTAINS
%token<std::string_view> DISTINCT
%token<std::string_view> EXTRACT
%token<std::string_view> REQUIRE
%token<std::string_view> COLLECT
%token<std::string_view> STARTS
%token<std::string_view> UNIQUE
%token<std::string_view> FILTER
%token<std::string_view> SINGLE
%token<std::string_view> SCALAR
%token<std::string_view> UNWIND
%token<std::string_view> REMOVE
%token<std::string_view> RETURN
%token<std::string_view> CREATE
%token<std::string_view> DELETE
%token<std::string_view> DETACH
%token<std::string_view> EXISTS
%token<std::string_view> IS_NOT
%token<std::string_view> LIMIT
%token<std::string_view> YIELD
%token<std::string_view> MATCH
%token<std::string_view> MERGE
%token<std::string_view> ORDER
%token<std::string_view> WHERE
%token<std::string_view> UNION
%token<std::string_view> FALSE
%token<std::string_view> COUNT
%token<std::string_view> DESC
%token<std::string_view> CALL
%token<std::string_view> NULL_
%token<std::string_view> TRUE
%token<std::string_view> WHEN
%token<std::string_view> NONE
%token<std::string_view> THEN
%token<std::string_view> ELSE
%token<std::string_view> CASE
%token<std::string_view> ENDS
%token<std::string_view> DROP
%token<std::string_view> SKIP
%token<std::string_view> WITH
%token<std::string_view> ANY
%token<std::string_view> SET
%token<std::string_view> ALL
%token<std::string_view> ASC
%token<std::string_view> NOT
%token<std::string_view> END
%token<std::string_view> XOR
%token<std::string_view> FOR
%token<std::string_view> ADD
%token<std::string_view> AND
%token<std::string_view> OR
%token<std::string_view> IN
%token<std::string_view> IS
%token<std::string_view> BY
%token<std::string_view> DO
%token<std::string_view> OF
%token<std::string_view> ON
%token<std::string_view> IF
%token<std::string_view> AS

%token<std::string_view> ESC_LITERAL
%token<std::string_view> STRING_LITERAL
%token<std::string_view> ID
%token<char> CHAR_LITERAL
%token<int64_t> DIGIT
%token<float> FLOAT

%token UNKNOWN

%type<db::v2::Literal> numLit boolLit charLit literal stringLit mapLit
%type<std::pair<std::string_view, db::v2::Expression*>> mapPair
%type<db::v2::MapLiteral*> mapPairChain
%type<db::v2::Symbol> symbol
%type<db::v2::QualifiedName> qualifiedName
%type<db::v2::QualifiedName> invocationName
%type<std::string_view> name
%type<std::string_view> reservedWord

%type<std::vector<std::string_view>> nodeLabels
%type<std::vector<std::string_view>> edgeTypes
%type<db::v2::MapLiteral*> properties

%type<std::optional<Symbol>> opt_symbol
%type<std::optional<std::vector<std::string_view>>> opt_nodeLabels
%type<db::v2::MapLiteral*> opt_properties
%type<std::optional<std::vector<std::string_view>>> opt_edgeTypes

%type<db::v2::Expression*> expression
%type<db::v2::Expression*> xorExpression
%type<db::v2::Expression*> andExpression
%type<db::v2::Expression*> notExpression
%type<db::v2::Expression*> comparisonExpression
%type<db::v2::Expression*> addSubExpression
%type<db::v2::Expression*> multDivExpression
%type<db::v2::Expression*> powerExpression
%type<db::v2::Expression*> unaryAddSubExpression
%type<db::v2::Expression*> atomicExpression
%type<db::v2::Expression*> listExpression
%type<db::v2::Expression*> stringExpression
%type<db::v2::Expression*> propertyOrLabelExpression
%type<db::v2::Expression*> propertyExpression
%type<db::v2::Expression*> atomExpression
%type<db::v2::Expression*> collectExpression
%type<db::v2::Expression*> pathExpression
%type<db::v2::Expression*> parenthesizedExpression

%type<db::v2::Expression*> projectionItem
%type<db::v2::Projection*> projectionItems
%type<db::v2::Projection*> projectionBody

%type<db::v2::BinaryOperator> comparisonSign
%type<db::v2::StringOperator> stringExpPrefix

%type<db::v2::Pattern*> pattern
%type<db::v2::Pattern*> patternWhere
%type<db::v2::PatternElement*> patternPart
%type<db::v2::PatternElement*> patternElem
%type<db::v2::PatternElement*> pathExpressionElem
%type<db::v2::NodePattern*> nodePattern
%type<db::v2::EdgePattern*> edgePattern
%type<std::tuple<std::optional<db::v2::Symbol>, std::optional<std::vector<std::string_view>>, db::v2::MapLiteral*>> edgeDetail
%type<std::pair<db::v2::EdgePattern*, db::v2::NodePattern*>> patternElemChain
%type<db::v2::WhereClause> where

%type<db::v2::SinglePartQuery*> singlePartQ
%type<db::v2::QueryCommand*> singleQuery
%type<db::v2::QueryCommand*> query
%type<db::v2::Statement*> readingStatement
%type<db::v2::Match*> matchSt
%type<db::v2::Skip*> skipSSt
%type<db::v2::Limit*> limitSSt
%type<db::v2::Return*> returnSt

%type<db::v2::Skip*> opt_skipSSt
%type<db::v2::Limit*> opt_limitSSt
%type<bool> opt_distinct

%expect 0

%start script

%%

script
    : queries
    | queries SEMI_COLON
    ;

queries
    : query {}
    | queries SEMI_COLON query {}
    ;

query
    : singleQuery
    | singleQuery unionList { scanner.notImplemented(@$, "Query + Unions"); }
    ;

unionList
    : unionSt { scanner.notImplemented(@$, "Unions"); }
    | unionList unionSt
    ;

singleQuery
    : singlePartQ { $$ = $1; }
    | multiPartQ { scanner.notImplemented(@$, "Multi-part queries"); }
    | createConstraint { scanner.notImplemented(@$, "CREATE CONSTRAINT"); }
    | dropConstraint { scanner.notImplemented(@$, "DROP CONSTRAINT"); }
    ;

returnSt
    : RETURN projectionBody { $$ = ast.newStatement<Return>($2); LOC($$, @$);}
    ;

withSt
    : WITH projectionBody where { scanner.notImplemented(@$, "WITH ... WHERE"); }
    | WITH projectionBody { scanner.notImplemented(@$, "WITH"); }
    ;

skipSSt
    : SKIP expression { $$ = ast.newSubStatement<Skip>($2); LOC($$, @$);}
    ;

limitSSt
    : LIMIT expression { $$ = ast.newSubStatement<Limit>($2); LOC($$, @$); }
    ;

projectionBody
    : opt_distinct projectionItems opt_orderSSt opt_skipSSt opt_limitSSt {
        $$ = $2;
        $$->setDistinct($1);
        $$->setSkip($4);
        $$->setLimit($5);
      }
    ;

opt_distinct
    : DISTINCT { $$ = true; }
    | { $$ = false; }
    ;

opt_orderSSt
    : orderSSt { scanner.notImplemented(@$, "ORDER BY"); }
    | /* empty */
    ;

opt_skipSSt
    : skipSSt { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_limitSSt
    : limitSSt { $$ = $1; }
    | { $$ = nullptr; }
    ;

projectionItems
    : MULT { $$ = ast.newProjection(); $$->setAll(); LOC($$, @$); }
    | projectionItem { $$ = ast.newProjection(); $$->add($1); LOC($$, @$); }
    | projectionItems COMMA projectionItem { $$ = $1; $$->add($3); }
    ;

projectionItem
    : expression
    | expression AS name { scanner.notImplemented(@$, "AS"); }
    ;

orderItem
    : expression
    | expression ASCENDING { scanner.notImplemented(@$, "ASCENDING"); }
    | expression ASC { scanner.notImplemented(@$, "ASC"); }
    | expression DESCENDING { scanner.notImplemented(@$, "DESCENDING"); }
    | expression DESC { scanner.notImplemented(@$, "DESC"); }
    ;

orderSSt
    : ORDER BY orderItem { scanner.notImplemented(@$, "ORDER BY"); }
    | orderSSt COMMA orderItem
    ;

singlePartQ
    : returnSt { scanner.notImplemented(@$, "Single part query: Return only"); }
    | updatingStatements { scanner.notImplemented(@$, "Single part query: Update only"); }
    | updatingStatements returnSt { scanner.notImplemented(@$, "Single part query: Update + Return"); }
    | readingStatements returnSt { $$ = ast.newSinglePartQuery(); LOC($$, @$); }
    | readingStatements updatingStatements { scanner.notImplemented(@$, "Single part query: Reading statement + Update"); }
    | readingStatements updatingStatements returnSt { scanner.notImplemented(@$, "Single part query: Reading statement + Update + Return"); }
    ;

readingStatements
    : readingStatement { }
    | readingStatements readingStatement { }
    ;

updatingStatements
    : updatingStatement { scanner.notImplemented(@$, "Updating statement"); }
    | updatingStatements updatingStatement { scanner.notImplemented(@$, "Multiple updating statements"); }
    ;
 
multiPartQ
    : updateWithSt singlePartQ { scanner.notImplemented(@$, "Update + With + Reading"); }
    | readingStatements updateWithSt singlePartQ { scanner.notImplemented(@$, "Reading + With + Update + Reading"); }
    ;

updateWithSt
    : updatingStatements withSt { scanner.notImplemented(@$, "Update + With"); }
    | withSt { scanner.notImplemented(@$, "With"); }
    | updateWithSt updatingStatements withSt { scanner.notImplemented(@$, "Update + With + Update"); }
    | updateWithSt withSt { scanner.notImplemented(@$, "Update + With"); }
    ;

matchSt
    : MATCH patternWhere opt_orderSSt opt_skipSSt opt_limitSSt { $$ = ast.newStatement<Match>($2, $4, $5); LOC($$, @$); }
    | OPTIONAL MATCH patternWhere opt_orderSSt opt_skipSSt opt_limitSSt { $$ = ast.newStatement<Match>($3, $5, $6, true); LOC($$, @$); }
    ;

unwindSt
    : UNWIND expression AS symbol { scanner.notImplemented(@$, "UNWIND"); }
    ;

readingStatement
    : matchSt { }
    | unwindSt { scanner.notImplemented(@$, "UNWIND"); }
    | queryCallSt { scanner.notImplemented(@$, "CALL"); }
    ;

updatingStatement
    : createSt { scanner.notImplemented(@$, "CREATE"); }
    | mergeSt { scanner.notImplemented(@$, "MERGE"); }
    | deleteSt { scanner.notImplemented(@$, "DELETE"); }
    | setSt { scanner.notImplemented(@$, "SET"); }
    | removeSt { scanner.notImplemented(@$, "REMOVE"); }
    ;
 
deleteSt
    : DELETE expressionChain { scanner.notImplemented(@$, "DELETE"); }
    | DETACH DELETE expressionChain { scanner.notImplemented(@$, "DETACH DELETE"); }
    ;

removeSt
    : REMOVE removeItemChain { scanner.notImplemented(@$, "REMOVE"); }
    ;

removeItemChain
    : removeItem { scanner.notImplemented(@$, "REMOVE"); }
    | removeItemChain COMMA removeItem { scanner.notImplemented(@$, "REMOVE multiple items"); }
    ;

removeItem
    : symbol nodeLabels { scanner.notImplemented(@$, "REMOVE"); }
    | propertyExpression { scanner.notImplemented(@$, "REMOVE"); }
    ;

queryCallSt
    : CALL invocationName parenExpressionChain { scanner.notImplemented(@$, "CALL name(...)"); }
    | CALL invocationName parenExpressionChain yieldClause { scanner.notImplemented(@$, "CALL name(...) YIELD ..."); }
    | OPTIONAL CALL invocationName parenExpressionChain { scanner.notImplemented(@$, "OPTIONAL CALL name(...)"); }
    | OPTIONAL CALL invocationName parenExpressionChain yieldClause { scanner.notImplemented(@$, "OPTIONAL CALL name(...) YIELD ..."); }
    | CALL OBRACE query CBRACE { scanner.notImplemented(@$, "CALL { subquery }"); }
    | CALL OPAREN CPAREN OBRACE query CBRACE { scanner.notImplemented(@$, "CALL () { subquery }"); }
    | CALL OPAREN callCapture CPAREN OBRACE query CBRACE { scanner.notImplemented(@$, "CALL (..) { subquery }"); }
    ;

callCapture
    : symbol { scanner.notImplemented(@$, "CALL capture"); }
    | symbol AS symbol { scanner.notImplemented(@$, "CALL capture"); }
    | callCapture COMMA symbol { scanner.notImplemented(@$, "CALL capture"); }
    | callCapture COMMA symbol AS symbol { scanner.notImplemented(@$, "CALL capture"); }
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
    : MERGE patternPart { scanner.notImplemented(@$, "MERGE"); }
    | MERGE patternPart mergeActionChain { scanner.notImplemented(@$, "MERGE"); }
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
    : SET setItem { scanner.notImplemented(@$, "SET"); }
    | setSt COMMA setItem
    ;

setItem
    : propertyExpression ASSIGN expression
    | symbol ADD_ASSIGN expression
    | symbol nodeLabels
    ;

nodeLabels
    : COLON name { $$ = { $2 }; }
    | COLON parameter { scanner.notImplemented(@$, "Parameters"); }
    | nodeLabels COLON name { $$ = std::move($1); $$.push_back($3); }
    | nodeLabels COLON parameter { $$ = std::move($1); scanner.notImplemented(@$, "Parameters"); }
    ;

createSt
    : CREATE pattern { scanner.notImplemented(@$, "CREATE"); }
    ;

patternWhere
    : pattern { $$ = $1; }
    | pattern where { $1->setWhere($2); $$ = $1; }
    ;

where
    : WHERE expression { $$.setExpression($2); }
    ;

pattern
    : patternPart { $$ = ast.newPattern(); $$->addElem($1); LOC($$, @$); }
    | pattern COMMA patternPart { $$ = $1, $$->addElem($3); }
    ;

expression
    : xorExpression { $$ = $1; }
    | expression OR xorExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Or, $3); LOC($$, @$); }
    ;

xorExpression
    : andExpression { $$ = $1; }
    | xorExpression XOR andExpression  { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Xor, $3); LOC($$, @$); }
    ;

andExpression
    : notExpression { $$ = $1; }
    | andExpression AND notExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::And, $3); LOC($$, @$); }
    ;

notExpression
    : comparisonExpression { $$ = $1; }
    | NOT notExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Not, $2); LOC($$, @$); }
    ;

comparisonExpression
    : addSubExpression { $$ = $1; }
    | comparisonExpression comparisonSign addSubExpression { $$ = ast.newExpression<BinaryExpression>($1, $2, $3); LOC($$, @$); }
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
    | IN { $$ = BinaryOperator::In; }
    ;

addSubExpression
    : multDivExpression { $$ = $1; }
    | addSubExpression PLUS multDivExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Add, $3); LOC($$, @$); }
    | addSubExpression SUB multDivExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Sub, $3); LOC($$, @$); }
    ;

multDivExpression
    : powerExpression { $$ = $1; }
    | multDivExpression MULT powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Mult, $3); LOC($$, @$); }
    | multDivExpression DIV powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Div, $3); LOC($$, @$); }
    | multDivExpression MOD powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Mod, $3); LOC($$, @$); }
    ;

powerExpression
    : stringExpression { $$ = $1; }
    | powerExpression CARET stringExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Pow, $3); LOC($$, @$); }
    ;

stringExpression
    : unaryAddSubExpression { $$ = $1; }
    | stringExpression stringExpPrefix unaryAddSubExpression { $$ = ast.newExpression<StringExpression>($1, $2, $3); LOC($$, @$); }
    ;

stringExpPrefix
    : STARTS WITH { $$ = StringOperator::StartsWith; }
    | ENDS WITH { $$ = StringOperator::EndsWith; }
    | CONTAINS { $$ = StringOperator::Contains; }
    ;

unaryAddSubExpression
    : atomicExpression { $$ = $1; }
    | PLUS unaryAddSubExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Plus, $2); LOC($$, @$); }
    | SUB unaryAddSubExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Minus, $2); LOC($$, @$); }
    // this allows chaining operators like -+--1 (which is +1)
    ;

atomicExpression
    : propertyOrLabelExpression { $$ = $1; }
    | atomicExpression listExpression { $$ = nullptr; scanner.notImplemented(@$, "List expressions"); }
    ;

listExpression
    : OBRACK expression CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK expression CBRACK"); }
    | OBRACK expression RANGE expression CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK expression RANGE expression CBRACK"); }
    | OBRACK RANGE expression CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK RANGE expression CBRACK"); }
    | OBRACK expression RANGE CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK expression RANGE CBRACK"); }
    | OBRACK RANGE CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK RANGE CBRACK"); }
    ;

propertyOrLabelExpression
    : propertyExpression { $$ = $1; }

    // | propertyExpression nodeLabels
    // This seems too permissive, it allows 'n.name:Person' which is weird

    // Replaced by this more specific rule
    | symbol nodeLabels { $$ = ast.newExpression<NodeLabelExpression>($1, std::move($2)); LOC($$, @$); }
    ;

propertyExpression
    : atomExpression { $$ = $1; }
    | qualifiedName DOT name { $1.addName($3); $$ = ast.newExpression<PropertyExpression>(std::move($1)); LOC($$, @$); }
    ;

atomExpression
    : pathExpression { $$ = $1; }
    | literal { $$ = ast.newExpression<LiteralExpression>($1); LOC($$, @$); }
    | symbol { $$ = ast.newExpression<SymbolExpression>($1); LOC($$, @$); }

    | parameter { scanner.notImplemented(@$, "Parameters"); }
    | caseExpression { scanner.notImplemented(@$, "CASE"); }
    | countFunc { scanner.notImplemented(@$, "COUNT"); }
    | listComprehension { scanner.notImplemented(@$, "List comprehensions"); }
    //| patternComprehension { scanner.notImplemented(@$, "Pattern comprehensions"); }
    | filterWith { scanner.notImplemented(@$, "Filter keywords"); }
    | functionInvocation { scanner.notImplemented(@$, "Function invocations"); }
    | subqueryExist { scanner.notImplemented(@$, "EXISTS"); }
    | collectExpression
    ;

collectExpression
    : COLLECT OBRACE readingStatements returnSt CBRACE { scanner.notImplemented(@$, "COLLECT"); }
    | COLLECT OPAREN expression CPAREN { scanner.notImplemented(@$, "COLLECT"); }
    | COLLECT OPAREN DISTINCT expression CPAREN { scanner.notImplemented(@$, "COLLECT"); }
    ;

patternPart
    : patternElem { $$ = $1; }
    | patternAlias { scanner.notImplemented(@$, "Pattern alias: Symbol = ()-[]-()-[]-()..."); }
    ;

patternAlias
    : symbol ASSIGN patternElem { scanner.notImplemented(@$, "Pattern alias: Symbol = ()-[]-()-[]-()..."); }

patternElem
    : nodePattern { $$ = ast.newPatternElem(); $$->addNode($1); LOC($$, @$); }
    | patternElem patternElemChain { $$ = $1; $$->addEdge($2.first); $$->addNode($2.second); }
    ;

patternElemChain
    : edgePattern nodePattern { $$ = std::make_pair($1, $2); }
    ;

properties
    : mapLit { $$ = *$1.as<MapLiteral*>(); }
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN { $$ = ast.newNode($2, std::move($3), $4); LOC($$, @$); }
    ;

opt_symbol
    : symbol { $$ = $1; }
    | { $$ = std::nullopt; }
    ;

opt_nodeLabels
    : nodeLabels { $$ = std::move($1); }
    | { $$ = std::nullopt; }
    ;

opt_properties
    : properties { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_edgeTypes
    : edgeTypes { $$ = std::move($1); }
    | { $$ = std::nullopt; }
    ;

opt_rangeLit
    : rangeLit
    | /* empty */
    ;

//lhs
//    : symbol ASSIGN
//    ;


edgePattern
    : TAIL_TAIL     { $$ = ast.newOutEdge(std::nullopt, std::nullopt, nullptr); LOC($$, @$); }
    | TIP_TAIL_TAIL { $$ = ast.newInEdge(std::nullopt, std::nullopt, nullptr); LOC($$, @$); }
    | TAIL_TAIL_TIP { $$ = ast.newOutEdge(std::nullopt, std::nullopt, nullptr); LOC($$, @$); }
    | TAIL_BRACKET edgeDetail BRACKET_TAIL     { $$ = ast.newOutEdge(std::get<0>($2), std::move(std::get<1>($2)), std::get<2>($2)); LOC($$, @$); }
    | TIP_TAIL_BRACKET edgeDetail BRACKET_TAIL { $$ = ast.newInEdge(std::get<0>($2), std::move(std::get<1>($2)), std::get<2>($2)); LOC($$, @$); }
    | TAIL_BRACKET edgeDetail BRACKET_TAIL_TIP { $$ = ast.newOutEdge(std::get<0>($2), std::move(std::get<1>($2)), std::get<2>($2)); LOC($$, @$); }
    ;

edgeDetail
    : opt_symbol opt_edgeTypes opt_rangeLit opt_properties { $$ = std::make_tuple($1, std::move($2), $4); }
    ;

edgeTypes
    : COLON name { $$ = { $2 }; }
    | edgeTypes PIPE name { scanner.notImplemented(@$, "EdgeType | EdgeType | ..."); }
    | edgeTypes PIPE COLON name { scanner.notImplemented(@$, "EdgeType | EdgeType | ..."); }
    ;

unionSt
    : UNION singleQuery { scanner.notImplemented(@$, "UNION"); }
    | UNION ALL singleQuery { scanner.notImplemented(@$, "UNION ALL"); }
    ;

subqueryExist
    : EXISTS OBRACE query CBRACE { scanner.notImplemented(@$, "EXISTS"); }
    | EXISTS OBRACE patternWhere CBRACE { scanner.notImplemented(@$, "EXISTS"); }
    ;

qualifiedName
    : symbol { $$.addName($1._name); }
    | qualifiedName DOT symbol { $$.addName($3._name); }
    ;

invocationName
    : qualifiedName { $$ = $1; }
    ;

functionInvocation
    : invocationName OPAREN CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN DISTINCT CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN expressionChain CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN DISTINCT expressionChain CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    ;

pathExpression
    : parenthesizedExpression { $$ = $1; }
    | OPAREN CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($3); LOC($$, @$); }
    | OPAREN symbol properties CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($5); $5->addRootNode(ast.newNode($2, std::nullopt, $3)); LOC($$, @$); }
    | OPAREN symbol nodeLabels properties CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($6); $6->addRootNode(ast.newNode($2, std::move($3), $4)); LOC($$, @$); }
    | OPAREN nodeLabels CPAREN pathExpressionElem {
        $$ = ast.newExpression<PathExpression>($4);
        auto* node = ast.newNode(std::nullopt, std::move($2), nullptr);
        $4->addRootNode(node);
        LOC($$, @$);
        LOC(node, @$);
      }
    | OPAREN nodeLabels properties CPAREN pathExpressionElem {
        $$ = ast.newExpression<PathExpression>($5);
        auto* node = ast.newNode(std::nullopt, std::move($2), nullptr);
        $5->addRootNode(node);
        LOC($$, @$);
        LOC(node, @$);
      }

    // Those three expressions are tricky and cause conflicts with 'OPAREN expression CPAREN'

    //| OPAREN symbol nodeLabels CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized expressions"); }
    // Causes conflicts because 'symbol nodeLabels' is a valid expression (propertyOrLabelExpression)

    //| OPAREN symbol CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized expressions"); }
    // Causes conflicts because 'symbol' is a valid expression (atomExpression)

    //| OPAREN properties CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized expressions"); }
    // Causes conflicts because 'properties' is a valid expression (map literal)

    // Instead, they are handled by the rule below

    | OPAREN expression CPAREN pathExpressionElem {
          $$ = ast.newExpression<PathExpression>($4);

          if (auto* node = ast.nodeFromExpression($2)) {
              $4->addRootNode(node);
              LOC(node, @$);
          } else {
              error(@1, "Invalid path expression. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");
          }

          LOC($$, @$);
      }
    ;

pathExpressionElem
: patternElemChain { $$ = ast.newPatternElem(); $$->addRootNode($1.second); $$->addRootEdge($1.first); LOC($$, @$);}
    | pathExpressionElem patternElemChain { $$ = $1; $$->addRootNode($2.second); $$->addRootEdge($2.first); }
    ;

parenthesizedExpression
    : OPAREN expression CPAREN { $$ = $2; }
    ;

filterWith
    : filterKeyword OPAREN filterExpression CPAREN { scanner.notImplemented(@$, "Filters"); }
    ;

filterKeyword
    : ALL
    | ANY
    | NONE
    | SINGLE
    ;

//patternComprehension
//    : OBRACK edgesChainPattern PIPE expression CBRACK
//    | OBRACK lhs edgesChainPattern PIPE expression CBRACK
//    | OBRACK edgesChainPattern where PIPE expression CBRACK
//    | OBRACK lhs edgesChainPattern where PIPE expression CBRACK
//    ;

//edgesChainPattern
//    : nodePattern patternElemChain
//    | edgesChainPattern patternElemChain
//    ;

listComprehension
    : OBRACK filterExpression CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    | OBRACK filterExpression PIPE expression CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    ;

filterExpression
    : symbol IN expression { scanner.notImplemented(@$, "IN"); }
    | symbol IN expression where { scanner.notImplemented(@$, "IN"); }
    ;

countFunc
    : COUNT OPAREN MULT CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN DISTINCT CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN expressionChain CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN DISTINCT expressionChain CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OBRACE patternWhere CBRACE { scanner.notImplemented(@$, "COUNT"); }

    // Here, returnSt is mandatory for MATCH subqueries, as opposed to Neo4j's Cypher parser
    | COUNT OBRACE query CBRACE { scanner.notImplemented(@$, "COUNT"); }
    ;

caseExpression
    : CASE whenThenChain END { scanner.notImplemented(@$, "CASE"); }
    | CASE expression whenThenChain END  { scanner.notImplemented(@$, "CASE"); }
    | CASE whenThenChain ELSE expression END { scanner.notImplemented(@$, "CASE"); }
    | CASE expression whenThenChain ELSE expression END { scanner.notImplemented(@$, "CASE"); }
    ;

whenThenChain
    : whenThen
    | whenThenChain whenThen
    ;

whenThen
    : WHEN expression THEN expression
    ;

parameter
    : DOLLAR symbol { scanner.notImplemented(@$, "Parameters"); }
    | DOLLAR numLit { scanner.notImplemented(@$, "Parameters"); }
    ;

literal
    : boolLit { $$ = Literal($1); }
    | numLit { $$ = Literal($1); }
    | NULL_ { $$ = Literal(); }
    | stringLit { $$ = Literal($1); }
    | charLit { $$ = Literal($1); }
    | listLit { scanner.notImplemented(@$, "Lists"); }
    | mapLit { $$ = Literal(std::move($1)); }
    ;

rangeLit
    : MULT { scanner.notImplemented(@$, "Ranges"); }
    | MULT numLit { scanner.notImplemented(@$, "Ranges"); }
    | MULT RANGE { scanner.notImplemented(@$, "Ranges"); }
    | MULT RANGE numLit { scanner.notImplemented(@$, "Ranges"); }
    | MULT numLit RANGE { scanner.notImplemented(@$, "Ranges"); }
    | MULT numLit RANGE numLit { scanner.notImplemented(@$, "Ranges"); }
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
    : STRING_LITERAL { $$ = Literal($1); }
    ;

charLit
    : CHAR_LITERAL { $$ = Literal($1); }
    ;

listLit
    : OBRACK CBRACK { scanner.notImplemented(@$, "Lists"); }
    //| OBRACK expressionChain CBRACK // Enabling this causes conflicts
    // Instead using this: (simpler, too simple?)
    | OBRACK listLitItems CBRACK { scanner.notImplemented(@$, "Lists"); }
    ;

listLitItems
    : listLitItem
    | listLitItems COMMA listLitItem
    ;

listLitItem
    : literal
    | parameter { scanner.notImplemented(@$, "Parameters"); }
    | caseExpression { scanner.notImplemented(@$, "CASE"); }
    | countFunc { scanner.notImplemented(@$, "COUNT"); }
    | listComprehension { scanner.notImplemented(@$, "List comprehensions"); }
    //| patternComprehension { scanner.notImplemented(@$, "Pattern comprehensions"); }
    | filterWith { scanner.notImplemented(@$, "Filters"); }
    | parenthesizedExpression // Enabling this causes conflicts, not needed?
    | functionInvocation { scanner.notImplemented(@$, "Function invocations"); }
    | symbol
    | subqueryExist { scanner.notImplemented(@$, "EXISTS"); }
    ;

mapLit
    : OBRACE CBRACE { auto* map = ast.newMapLiteral(); $$ = Literal(map); LOC(map, @$); }
    | OBRACE mapPairChain CBRACE { $$ = Literal($2);}
    ;

mapPairChain
    : mapPair { $$ = ast.newMapLiteral(); $$->set($1.first, $1.second); LOC($$, @$); }
    | mapPairChain COMMA mapPair { $$ = $1; $$->set($3.first, $3.second); }
    ;

mapPair
    : name COLON expression { $$ = std::make_pair($1, $3); }
    ;

name
    : symbol { $$ = $1._name ; }
    | reservedWord { $$ = $1 ; }
    ;

symbol
    : ESC_LITERAL { $$ = Symbol { ._name = $1 }; }
    | ID { $$ = Symbol { ._name = $1 }; }
    | FILTER { $$ = Symbol { ._name = $1 }; }
    | EXTRACT { $$ = Symbol { ._name = $1 }; }
    //| ANY { $$ = Symbol { ._name = $1 }; } // Causes conflicts
    //| NONE { $$ = Symbol { ._name = $1 }; } // Causes conflicts
    //| SINGLE { $$ = Symbol { ._name = $1 }; } // Causes conflicts
    ;

createConstraint
    : CREATE CONSTRAINT symbol forConstraint
    | CREATE CONSTRAINT symbol IF NOT EXISTS forConstraint
    ;

forConstraint
    : FOR nodeConstraintPattern REQUIRE constraint
    | FOR edgeConstraintPattern REQUIRE constraint
    ;

nodeConstraintPattern
    : OPAREN symbol nodeLabels CPAREN
    ;

edgeConstraintPattern
    : OPAREN CPAREN TAIL_BRACKET symbol edgeTypes BRACKET_TAIL OPAREN CPAREN
    ;

constraint
    : qualifiedName DOT name IS UNIQUE
    | qualifiedName DOT name IS NOT NULL_
    ;

dropConstraint
    : DROP CONSTRAINT symbol
    | DROP CONSTRAINT symbol IF EXISTS
    ;

reservedWord
    : ALL { $$ = $1; }
    | ASC { $$ = $1; }
    | ASCENDING { $$ = $1; }
    | BY { $$ = $1; }
    | CREATE { $$ = $1; }
    | DELETE { $$ = $1; }
    | DESC { $$ = $1; }
    | DESCENDING { $$ = $1; }
    | DETACH { $$ = $1; }
    | EXISTS { $$ = $1; }
    | LIMIT { $$ = $1; }
    | MATCH { $$ = $1; }
    | MERGE { $$ = $1; }
    | ON { $$ = $1; }
    | IF { $$ = $1; }
    | OPTIONAL { $$ = $1; }
    | ORDER { $$ = $1; }
    | REMOVE { $$ = $1; }
    | RETURN { $$ = $1; }
    | SET { $$ = $1; }
    | SKIP { $$ = $1; }
    | WHERE { $$ = $1; }
    | WITH { $$ = $1; }
    | UNION { $$ = $1; }
    | UNWIND { $$ = $1; }
    | AND { $$ = $1; }
    | AS { $$ = $1; }
    | CONTAINS { $$ = $1; }
    | DISTINCT { $$ = $1; }
    | ENDS { $$ = $1; }
    | IN { $$ = $1; }
    | IS { $$ = $1; }
    | NOT { $$ = $1; }
    | OR { $$ = $1; }
    | STARTS { $$ = $1; }
    | XOR { $$ = $1; }
    | FALSE { $$ = $1; }
    | TRUE { $$ = $1; }
    | NULL_ { $$ = $1; }
    | CONSTRAINT { $$ = $1; }
    | DO { $$ = $1; }
    | FOR { $$ = $1; }
    | REQUIRE { $$ = $1; }
    | COLLECT { $$ = $1; }
    | UNIQUE { $$ = $1; }
    | CASE { $$ = $1; }
    | WHEN { $$ = $1; }
    | THEN { $$ = $1; }
    | ELSE { $$ = $1; }
    | END { $$ = $1; }
    | MANDATORY { $$ = $1; }
    | SCALAR { $$ = $1; }
    | OF { $$ = $1; }
    | ADD { $$ = $1; }
    | DROP { $$ = $1; }
    ;

%%

void db::v2::YCypherParser::error(const location_type& l, const std::string& m) {
    scanner.syntaxError(l, m);
}


