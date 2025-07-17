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
    // Inspired by https://github.com/antlr/grammars-v4/blob/master/cypher/CypherParser.g4

    #include <spdlog/fmt/bundled/core.h>
    #include <optional>
    #include <vector>

    #include "expressions/All.h"
    #include "Literal.h"
    #include "Symbol.h"
    #include "WhereClause.h"
    #include "pattern/Pattern.h"
    #include "pattern/PatternNode.h"
    #include "pattern/PatternEdge.h"
    #include "statements/ReadingStatementContainer.h"
    #include "expressions/PathExpression.h"
    #include "statements/Return.h"
    #include "SinglePartQuery.h"
    #include "Projection.h"

    namespace db {
        class YCypherScanner;
        class CypherAST;
        class QualifiedName;
    }
}

%code top {
    #include "YCypherScanner.h"
    #include "GeneratedCypherParser.h"
    #include "CypherAST.h"
    #include "QualifiedName.h"

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
%token<std::string> COLLECT
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
%token<std::string> IF
%token<std::string> AS

%token<std::string> ESC_LITERAL
%token<std::string> STRING_LITERAL
%token<std::string> ID
%token<char> CHAR_LITERAL
%token<int64_t> DIGIT
%token<float> FLOAT

%token UNKNOWN

%type<db::Literal> numLit boolLit charLit literal stringLit mapLit
%type<std::pair<std::string, db::Expression*>> mapPair
%type<db::MapLiteral*> mapPairChain
%type<db::Symbol> symbol
%type<db::QualifiedName> qualifiedName
%type<db::QualifiedName> invocationName
%type<std::string> name
%type<std::string> reservedWord

%type<std::vector<std::string>> nodeLabels
%type<std::vector<std::string>> edgeTypes
%type<db::MapLiteral*> properties

%type<std::optional<Symbol>> opt_symbol
%type<std::optional<std::vector<std::string>>> opt_nodeLabels
%type<std::optional<MapLiteral*>> opt_properties
%type<std::optional<std::vector<std::string>>> opt_edgeTypes

%type<db::Expression*> expression
%type<db::Expression*> xorExpression
%type<db::Expression*> andExpression
%type<db::Expression*> notExpression
%type<db::Expression*> comparisonExpression
%type<db::Expression*> addSubExpression
%type<db::Expression*> multDivExpression
%type<db::Expression*> powerExpression
%type<db::Expression*> unaryAddSubExpression
%type<db::Expression*> atomicExpression
%type<db::Expression*> listExpression
%type<db::Expression*> stringExpression
%type<db::Expression*> propertyOrLabelExpression
%type<db::Expression*> propertyExpression
%type<db::Expression*> atomExpression
%type<db::Expression*> collectExpression
%type<db::Expression*> pathExpression
%type<db::Expression*> parenthesizedExpression

%type<db::Expression*> projectionItem
%type<db::Projection*> projectionItems
%type<db::Projection*> projectionBody

%type<BinaryOperator> comparisonSign
%type<StringOperator> stringExpPrefix

%type<db::Pattern*> pattern
%type<db::Pattern*> patternWhere
%type<db::PatternPart*> patternPart
%type<db::PatternPart*> patternElem
%type<db::PatternPart*> pathExpressionElem
%type<db::PatternNode*> nodePattern
%type<db::PatternEdge*> edgePattern
%type<std::pair<db::PatternEdge*, db::PatternNode*>> patternElemChain
%type<db::WhereClause> where

%type<db::SinglePartQuery*> singlePartQ
%type<db::Query*> singleQuery
%type<db::Query*> query
%type<db::ReadingStatementContainer*> readingStatements
%type<db::ReadingStatement> readingStatement
%type<db::Match*> matchSt
%type<db::Skip*> skipSt
%type<db::Limit*> limitSt
%type<db::Return*> returnSt

%type<db::Skip*> opt_skipSt
%type<db::Limit*> opt_limitSt
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
    | singleQuery unionList { scanner.notImplemented("Query + Unions"); }
    ;

unionList
    : unionSt { scanner.notImplemented("Unions"); }
    | unionList unionSt
    ;

singleQuery
    : singlePartQ { $$ = $1; }
    | multiPartQ { scanner.notImplemented("Multi-part queries"); }
    | createConstraint { scanner.notImplemented("CREATE CONSTRAINT"); }
    | dropConstraint { scanner.notImplemented("DROP CONSTRAINT"); }
    ;

returnSt
    : RETURN projectionBody { $$ = ast.newStatement<Return>($2); }
    ;

withSt
    : WITH projectionBody where { scanner.notImplemented("WITH ... WHERE"); }
    | WITH projectionBody { scanner.notImplemented("WITH"); }
    ;

skipSt
    : SKIP expression { $$ = ast.newStatement<Skip>($2); }
    ;

limitSt
    : LIMIT expression { $$ = ast.newStatement<Limit>($2); }
    ;

projectionBody
    : opt_distinct projectionItems opt_orderSt opt_skipSt opt_limitSt {
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

opt_orderSt
    : orderSt { scanner.notImplemented("ORDER BY"); }
    | /* empty */
    ;

opt_skipSt
    : skipSt { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_limitSt
    : limitSt { $$ = $1; }
    | { $$ = nullptr; }
    ;

projectionItems
    : MULT { $$ = ast.newProjection(); $$->setAll(); }
    | projectionItem { $$ = ast.newProjection(); $$->add($1); }
    | projectionItems COMMA projectionItem { $$ = $1; $$->add($3); }
    ;

projectionItem
    : expression
    | expression AS name { scanner.notImplemented("AS"); }
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
    : returnSt { scanner.notImplemented("Single part query: Return only"); }
    | updatingStatements { scanner.notImplemented("Single part query: Update only"); }
    | updatingStatements returnSt { scanner.notImplemented("Single part query: Update + Return"); }
    | readingStatements returnSt { 
        $$ = ast.newSinglePartQuery();
        $$->setReadingStatements($1);
        $$->setReturn($2);
      }
    | readingStatements updatingStatements { scanner.notImplemented("Single part query: Reading statement + Update"); }
    | readingStatements updatingStatements returnSt { scanner.notImplemented("Single part query: Reading statement + Update + Return"); }
    ;

readingStatements
    : readingStatement { $$ = ast.newReadingStatementContainer(); $$->add($1); }
    | readingStatements readingStatement { $$ = $1; $$->add($2); }
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
    : MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt { $$ = ast.newStatement<Match>($2, $4, $5); }
    | OPTIONAL MATCH patternWhere opt_orderSt opt_skipSt opt_limitSt { $$ = ast.newStatement<Match>($3, $5, $6, true); }
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
    : pattern { $$ = $1; }
    | pattern where { $1->setWhere($2); $$ = $1; }
    ;

where
    : WHERE expression { $$.setExpression($2); }
    ;

pattern
    : patternPart { $$ = ast.newPattern(); $$->addPart($1); }
    | pattern COMMA patternPart { $$ = $1, $$->addPart($3); }
    ;

expression
    : xorExpression { $$ = $1; }
    | expression OR xorExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Or, $3); }
    ;

xorExpression
    : andExpression { $$ = $1; }
    | xorExpression XOR andExpression  { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Xor, $3); }
    ;

andExpression
    : notExpression { $$ = $1; }
    | andExpression AND notExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::And, $3); }
    ;

notExpression
    : comparisonExpression { $$ = $1; }
    | NOT notExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Not, $2); }
    ;

comparisonExpression
    : addSubExpression { $$ = $1; }
    | comparisonExpression comparisonSign addSubExpression { $$ = ast.newExpression<BinaryExpression>($1, $2, $3); }
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
    | addSubExpression PLUS multDivExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Add, $3); }
    | addSubExpression SUB multDivExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Sub, $3); }
    ;

multDivExpression
    : powerExpression { $$ = $1; }
    | multDivExpression MULT powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Mult, $3); }
    | multDivExpression DIV powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Div, $3); }
    | multDivExpression MOD powerExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Mod, $3); }
    ;

powerExpression
    : stringExpression { $$ = $1; }
    | powerExpression CARET stringExpression { $$ = ast.newExpression<BinaryExpression>($1, BinaryOperator::Pow, $3); }
    ;

stringExpression
    : unaryAddSubExpression { $$ = $1; }
    | stringExpPrefix unaryAddSubExpression { $$ = ast.newExpression<StringExpression>($1, $2); }
    ;

stringExpPrefix
    : STARTS WITH { $$ = StringOperator::StartsWith; }
    | ENDS WITH { $$ = StringOperator::EndsWith; }
    | CONTAINS { $$ = StringOperator::Contains; }
    ;

unaryAddSubExpression
    : atomicExpression { $$ = $1; }
    | PLUS unaryAddSubExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Plus, $2); }
    | SUB unaryAddSubExpression { $$ = ast.newExpression<UnaryExpression>(UnaryOperator::Minus, $2); }
    // this allows chaining operators like -+--1 (which is +1)
    ;

atomicExpression
    : propertyOrLabelExpression { $$ = $1; }
    | atomicExpression listExpression { $$ = nullptr; scanner.notImplemented("List expressions"); }
    ;

listExpression
    : OBRACK expression CBRACK { $$ = nullptr; scanner.notImplemented("OBRACK expression CBRACK"); }
    | OBRACK expression RANGE expression CBRACK { $$ = nullptr; scanner.notImplemented("OBRACK expression RANGE expression CBRACK"); }
    | OBRACK RANGE expression CBRACK { $$ = nullptr; scanner.notImplemented("OBRACK RANGE expression CBRACK"); }
    | OBRACK expression RANGE CBRACK { $$ = nullptr; scanner.notImplemented("OBRACK expression RANGE CBRACK"); }
    | OBRACK RANGE CBRACK { $$ = nullptr; scanner.notImplemented("OBRACK RANGE CBRACK"); }
    ;

propertyOrLabelExpression
    : propertyExpression { $$ = $1; }

    // | propertyExpression nodeLabels
    // This seems too permissive, it allows 'n.name:Person' which is weird

    // Replaced by this more specific rule
    | symbol nodeLabels { $$ = ast.newExpression<NodeLabelExpression>(std::move($1), std::move($2)); }
    ;

propertyExpression
    : atomExpression { $$ = $1; }
    | qualifiedName DOT name { $1.addName(std::move($3)); $$ = ast.newExpression<PropertyExpression>(std::move($1)); }
    ;

atomExpression
    : pathExpression { $$ = $1; }
    | literal { $$ = ast.newExpression<AtomExpression>(std::move($1)); }
    | symbol { $$ = ast.newExpression<AtomExpression>(std::move($1)); }

    | parameter { scanner.notImplemented("Parameters"); }
    | caseExpression { scanner.notImplemented("CASE"); }
    | countFunc { scanner.notImplemented("COUNT"); }
    | listComprehension { scanner.notImplemented("List comprehensions"); }
    //| patternComprehension { scanner.notImplemented("Pattern comprehensions"); }
    | filterWith { scanner.notImplemented("Filter keywords"); }
    | functionInvocation { scanner.notImplemented("Function invocations"); }
    | subqueryExist { scanner.notImplemented("EXISTS"); }
    | collectExpression
    ;

collectExpression
    : COLLECT OBRACE readingStatements returnSt CBRACE { scanner.notImplemented("COLLECT"); }
    | COLLECT OPAREN expression CPAREN { scanner.notImplemented("COLLECT"); }
    | COLLECT OPAREN DISTINCT expression CPAREN { scanner.notImplemented("COLLECT"); }
    ;

patternPart
    : patternElem { $$ = $1; }
    | patternAlias { scanner.notImplemented("Pattern alias: Symbol = ()-[]-()-[]-()..."); }
    ;

patternAlias
    : symbol ASSIGN patternElem { scanner.notImplemented("Pattern alias: Symbol = ()-[]-()-[]-()..."); }

patternElem
    : nodePattern { $$ = ast.newPatternPart(); $$->addNode($1); }
    | patternElem patternElemChain { $$ = $1; $$->addEdge($2.first); $$->addNode($2.second); }
    ;

patternElemChain
    : edgePattern nodePattern { $$ = std::make_pair($1, $2); }
    ;

properties
    : mapLit { $$ = *$1.as<MapLiteral*>(); }
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN { $$ = ast.newNode(std::move($2), std::move($3)); }
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
    : properties { $$ = $1; }
    | { $$ = std::nullopt; }
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
    : TAIL_TAIL     { $$ = ast.newOutEdge(); } // --
    | TIP_TAIL_TAIL { $$ = ast.newInEdge(); }  // <--
    | TAIL_TAIL_TIP { $$ = ast.newOutEdge(); } // -->
    | TAIL_BRACKET edgeDetail BRACKET_TAIL     { $$ = ast.newOutEdge(); } // -[]->
    | TIP_TAIL_BRACKET edgeDetail BRACKET_TAIL { $$ = ast.newInEdge(); }  // <-[]-
    | TAIL_BRACKET edgeDetail BRACKET_TAIL_TIP { $$ = ast.newOutEdge(); } // -[]->
    ;

edgeDetail
    : opt_symbol opt_edgeTypes opt_rangeLit opt_properties
    ;

edgeTypes
    : COLON name { $$ = {std::move($2)}; }
    | edgeTypes PIPE name { scanner.notImplemented("EdgeType | EdgeType | ..."); }
    | edgeTypes PIPE COLON name { scanner.notImplemented("EdgeType | EdgeType | ..."); }
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
    : symbol { $$.addName(std::move($1._name)); }
    | qualifiedName DOT symbol { $$.addName(std::move($3._name)); }
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

pathExpression
    : parenthesizedExpression { $$ = $1; }
    | OPAREN CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($3); }
    | OPAREN symbol properties CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($5); $5->addRootNode(ast.newNode($2, std::nullopt)); }
    | OPAREN symbol nodeLabels properties CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($6); $6->addRootNode(ast.newNode($2, std::move($3))); }
    | OPAREN nodeLabels CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($4); $4->addRootNode(ast.newNode(std::nullopt, std::move($2))); }
    | OPAREN nodeLabels properties CPAREN pathExpressionElem { $$ = ast.newExpression<PathExpression>($5); $5->addRootNode(ast.newNode(std::nullopt, std::move($2))); }

    // Those three expressions are tricky and cause conflicts with 'OPAREN expression CPAREN'

    //| OPAREN symbol nodeLabels CPAREN patternElemChain { scanner.notImplemented("Parenthesized expressions"); }
    // Causes conflicts because 'symbol nodeLabels' is a valid expression (propertyOrLabelExpression)

    //| OPAREN symbol CPAREN patternElemChain { scanner.notImplemented("Parenthesized expressions"); }
    // Causes conflicts because 'symbol' is a valid expression (atomExpression)

    //| OPAREN properties CPAREN patternElemChain { scanner.notImplemented("Parenthesized expressions"); }
    // Causes conflicts because 'properties' is a valid expression (map literal)

    // Instead, they are handled by the rule below

    | OPAREN expression CPAREN pathExpressionElem {
          $$ = ast.newExpression<PathExpression>($4);

          if (auto* node = ast.nodeFromExpression($2)) {
              $4->addRootNode(node);
          } else {
              error(@1, "Invalid path expression. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");
          }
      }
    ;

pathExpressionElem
    : patternElemChain { $$ = ast.newPatternPart(); $$->addRootNode($1.second); $$->addRootEdge($1.first); }
    | pathExpressionElem patternElemChain { $$ = $1; $$->addRootNode($2.second); $$->addRootEdge($2.first); }
    ;

parenthesizedExpression
    : OPAREN expression CPAREN { $$ = $2; }
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
    : boolLit { $$ = Literal($1); }
    | numLit { $$ = Literal($1); }
    | NULL_ { $$ = Literal(std::nullopt); }
    | stringLit { $$ = Literal(std::move($1)); }
    | charLit { $$ = Literal($1); }
    | listLit { scanner.notImplemented("Lists"); }
    | mapLit { $$ = Literal(std::move($1)); }
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
    //| patternComprehension { scanner.notImplemented("Pattern comprehensions"); }
    | filterWith { scanner.notImplemented("Filters"); }
    | parenthesizedExpression // Enabling this causes conflicts, not needed?
    | functionInvocation { scanner.notImplemented("Function invocations"); }
    | symbol
    | subqueryExist { scanner.notImplemented("EXISTS"); }
    ;

mapLit
    : OBRACE CBRACE { $$ = Literal(ast.newMapLiteral()); }
    | OBRACE mapPairChain CBRACE { $$ = Literal($2);}
    ;

mapPairChain
    : mapPair { $$ = ast.newMapLiteral(); $$->set(std::move($1.first), $1.second); }
    | mapPairChain COMMA mapPair { $$ = $1; $$->set(std::move($3.first), $3.second); }
    ;

mapPair
    : name COLON expression { $$ = std::make_pair(std::move($1), $3); }
    ;

name
    : symbol { $$ = std::move($1._name) ; }
    | reservedWord { $$ = std::move($1) ; }
    ;

symbol
    : ESC_LITERAL { $$ = Symbol { ._name = std::move($1)}; }
    | ID { $$ = Symbol { ._name = std::move($1) }; }
    | FILTER { $$ = Symbol { ._name = std::move($1) }; }
    | EXTRACT { $$ = Symbol { ._name = std::move($1) }; }
    //| ANY { $$ = Symbol { ._name = std::move($1) }; } // Causes conflicts
    //| NONE { $$ = Symbol { ._name = std::move($1) }; } // Causes conflicts
    //| SINGLE { $$ = Symbol { ._name = std::move($1) }; } // Causes conflicts
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
    | IF { $$ = std::move($1); }
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
    | COLLECT { $$ = std::move($1); }
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


