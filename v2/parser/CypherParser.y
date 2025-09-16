%require "3.2"
%language "c++"

%define api.parser.class { YCypherParser }
%define api.value.type variant
%define parse.assert
%define parse.error detailed
%define api.namespace { db::v2 }
%parse-param {db::v2::YCypherScanner& scanner}
%parse-param {db::v2::CypherAST* ast}
%define api.location.type { SourceLocation }
%locations

%header
%verbose

%code requires {

    #include <optional>

    // Inspired by https://github.com/antlr/grammars-v4/blob/master/cypher/CypherParser.g4

    #define LOC(obj, loc) ast->setLocation(obj, loc)

    #include "SourceLocation.h"

    #include "stmt/StmtContainer.h"
    #include "stmt/ReturnStmt.h"
    #include "stmt/MatchStmt.h"
    #include "expr/All.h"
    #include "expr/Literal.h"
    #include "Symbol.h"
    #include "WhereClause.h"
    #include "Pattern.h"
    #include "NodePattern.h"
    #include "EdgePattern.h"
    #include "SinglePartQuery.h"
    #include "Projection.h"
    #include "PatternElement.h"
    #include "stmt/Skip.h"
    #include "stmt/Limit.h"

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

%type<db::v2::Literal*> numLit literal
%type<db::v2::BoolLiteral*> boolLit
%type<db::v2::CharLiteral*> charLit
%type<db::v2::StringLiteral*> stringLit
%type<db::v2::MapLiteral*> mapLit
%type<std::pair<db::v2::Symbol*, db::v2::Expr*>> mapPair
%type<db::v2::MapLiteral*> mapPairChain
%type<db::v2::Symbol*> symbol
%type<db::v2::QualifiedName*> qualifiedName
%type<db::v2::QualifiedName*> invocationName
%type<db::v2::Symbol*> name
%type<db::v2::Symbol*> reservedWord

%type<std::vector<db::v2::Symbol*>> nodeLabels
%type<std::vector<db::v2::Symbol*>> edgeTypes
%type<db::v2::MapLiteral*> properties

%type<db::v2::Symbol*> opt_symbol
%type<std::optional<std::vector<db::v2::Symbol*>>> opt_nodeLabels
%type<db::v2::MapLiteral*> opt_properties
%type<std::optional<std::vector<db::v2::Symbol*>>> opt_edgeTypes

%type<db::v2::Expr*> Expr
%type<db::v2::Expr*> xorExpr
%type<db::v2::Expr*> andExpr
%type<db::v2::Expr*> notExpr
%type<db::v2::Expr*> comparisonExpr
%type<db::v2::Expr*> addSubExpr
%type<db::v2::Expr*> multDivExpr
%type<db::v2::Expr*> powerExpr
%type<db::v2::Expr*> unaryAddSubExpr
%type<db::v2::Expr*> atomicExpr
%type<db::v2::Expr*> listExpr
%type<db::v2::Expr*> stringExpr
%type<db::v2::Expr*> propertyOrLabelExpr
%type<db::v2::Expr*> propertyExpr
%type<db::v2::Expr*> atomExpr
%type<db::v2::Expr*> collectExpr
%type<db::v2::Expr*> pathExpr
%type<db::v2::Expr*> parenthesizedExpr

%type<db::v2::Expr*> projectionItem
%type<db::v2::Projection*> projectionItems
%type<db::v2::Projection*> projectionBody

%type<db::v2::BinaryOperator> comparisonSign
%type<db::v2::StringOperator> stringExpPrefix

%type<db::v2::Pattern*> pattern
%type<db::v2::Pattern*> patternWhere
%type<db::v2::PatternElement*> patternPart
%type<db::v2::PatternElement*> patternElem
%type<db::v2::PatternElement*> pathExprElem
%type<db::v2::NodePattern*> nodePattern
%type<db::v2::EdgePattern*> edgePattern
%type<db::v2::EdgePattern*> edgeDetail
%type<std::pair<db::v2::EdgePattern*, db::v2::NodePattern*>> patternElemChain
%type<db::v2::WhereClause*> where

%type<db::v2::SinglePartQuery*> singlePartQuery
%type<db::v2::QueryCommand*> singleQuery
%type<db::v2::QueryCommand*> query
%type<db::v2::Stmt*> readingStatement
%type<db::v2::Stmt*> updatingStatement
%type<db::v2::StmtContainer*> readingStatements
%type<db::v2::StmtContainer*> updatingStatements
%type<db::v2::MatchStmt*> matchSt
%type<db::v2::Skip*> skipSSt
%type<db::v2::Limit*> limitSSt
%type<db::v2::ReturnStmt*> returnSt

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
    : singlePartQuery { $$ = $1; }
    | multiPartQuery { scanner.notImplemented(@$, "Multi-part queries"); }
    | createConstraint { scanner.notImplemented(@$, "CREATE CONSTRAINT"); }
    | dropConstraint { scanner.notImplemented(@$, "DROP CONSTRAINT"); }
    ;

returnSt
    : RETURN projectionBody { $$ = ReturnStmt::create(ast, $2); LOC($$, @$); }
    ;

withSt
    : WITH projectionBody where { scanner.notImplemented(@$, "WITH ... WHERE"); }
    | WITH projectionBody { scanner.notImplemented(@$, "WITH"); }
    ;

skipSSt
    : SKIP Expr { $$ = Skip::create(ast, $2); LOC($$, @$); }
    ;

limitSSt
    : LIMIT Expr { $$ = Limit::create(ast, $2); LOC($$, @$); }
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
    : MULT { $$ = Projection::create(ast); $$->setAll(); LOC($$, @$); }
    | projectionItem { $$ = Projection::create(ast); $$->add($1); LOC($$, @$); }
    | projectionItems COMMA projectionItem { $$ = $1; $$->add($3); }
    ;

projectionItem
    : Expr
    | Expr AS name { scanner.notImplemented(@$, "AS"); }
    ;

orderItem
    : Expr
    | Expr ASCENDING { scanner.notImplemented(@$, "ASCENDING"); }
    | Expr ASC { scanner.notImplemented(@$, "ASC"); }
    | Expr DESCENDING { scanner.notImplemented(@$, "DESCENDING"); }
    | Expr DESC { scanner.notImplemented(@$, "DESC"); }
    ;

orderSSt
    : ORDER BY orderItem { scanner.notImplemented(@$, "ORDER BY"); }
    | orderSSt COMMA orderItem
    ;

singlePartQuery
    : returnSt { $$ = SinglePartQuery::create(ast); $$->setReturnStmt($1); LOC($$, @$); }
    | updatingStatements { $$ = SinglePartQuery::create(ast); $$->setUpdateStmts($1); LOC($$, @$); }
    | updatingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setUpdateStmts($1); $$->setReturnStmt($2); LOC($$, @$); }
    | readingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setReturnStmt($2); LOC($$, @$); }
    | readingStatements updatingStatements { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setUpdateStmts($2); LOC($$, @$); }
    | readingStatements updatingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setUpdateStmts($2); $$->setReturnStmt($3); LOC($$, @$); }
    ;

readingStatements
    : readingStatement { $$ = StmtContainer::create(ast); $$->add($1); LOC($$, @$); }
    | readingStatements readingStatement { $$ = $1; $$->add($2); }
    ;

updatingStatements
    : updatingStatement { $$ = StmtContainer::create(ast); $$->add($1); LOC($$, @$); }
    | updatingStatements updatingStatement { $$ = $1; $$->add($2); }
    ;
 
multiPartQuery
    : updateWithSt singlePartQuery { scanner.notImplemented(@$, "Update + With + Reading"); }
    | readingStatements updateWithSt singlePartQuery { scanner.notImplemented(@$, "Reading + With + Update + Reading"); }
    ;

updateWithSt
    : updatingStatements withSt { scanner.notImplemented(@$, "Update + With"); }
    | withSt { scanner.notImplemented(@$, "With"); }
    | updateWithSt updatingStatements withSt { scanner.notImplemented(@$, "Update + With + Update"); }
    | updateWithSt withSt { scanner.notImplemented(@$, "Update + With"); }
    ;

matchSt
    : MATCH patternWhere opt_orderSSt opt_skipSSt opt_limitSSt { $$ = MatchStmt::create(ast, $2); $$->setSkip($4); $$->setLimit($5); LOC($$, @$); }
    | OPTIONAL MATCH patternWhere opt_orderSSt opt_skipSSt opt_limitSSt { $$ = MatchStmt::create(ast, $3); $$->setSkip($5); $$->setLimit($6); $$->setOptional(true); LOC($$, @$); }
    ;

unwindSt
    : UNWIND Expr AS symbol { scanner.notImplemented(@$, "UNWIND"); }
    ;

readingStatement
    : matchSt { $$ = $1; }
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
    : DELETE ExprChain { scanner.notImplemented(@$, "DELETE"); }
    | DETACH DELETE ExprChain { scanner.notImplemented(@$, "DETACH DELETE"); }
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
    | propertyExpr { scanner.notImplemented(@$, "REMOVE"); }
    ;

queryCallSt
    : CALL invocationName parenExprChain { scanner.notImplemented(@$, "CALL name(...)"); }
    | CALL invocationName parenExprChain yieldClause { scanner.notImplemented(@$, "CALL name(...) YIELD ..."); }
    | OPTIONAL CALL invocationName parenExprChain { scanner.notImplemented(@$, "OPTIONAL CALL name(...)"); }
    | OPTIONAL CALL invocationName parenExprChain yieldClause { scanner.notImplemented(@$, "OPTIONAL CALL name(...) YIELD ..."); }
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

ExprChain
    : Expr
    | ExprChain COMMA Expr
    ;

yieldClause
    : YIELD yieldItems
    | YIELD MULT
    ;

parenExprChain
    : OPAREN ExprChain CPAREN
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
    : propertyExpr ASSIGN Expr
    | symbol ADD_ASSIGN Expr
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
    : WHERE Expr { $$ = WhereClause::create(ast, $2); }
    ;

pattern
    : patternPart { $$ = Pattern::create(ast); $$->addElement($1); LOC($$, @$); }
    | pattern COMMA patternPart { $$ = $1, $$->addElement($3); }
    ;

Expr
    : xorExpr { $$ = $1; }
    | Expr OR xorExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Or, $1, $3); LOC($$, @$); }
    ;

xorExpr
    : andExpr { $$ = $1; }
    | xorExpr XOR andExpr  { $$ = BinaryExpr::create(ast, BinaryOperator::Xor, $1, $3); LOC($$, @$); }
    ;

andExpr
    : notExpr { $$ = $1; }
    | andExpr AND notExpr { $$ = BinaryExpr::create(ast, BinaryOperator::And, $1, $3); LOC($$, @$); }
    ;

notExpr
    : comparisonExpr { $$ = $1; }
    | NOT notExpr { $$ = UnaryExpr::create(ast, UnaryOperator::Not, $2); LOC($$, @$); }
    ;

comparisonExpr
    : addSubExpr { $$ = $1; }
    | comparisonExpr comparisonSign addSubExpr { $$ = BinaryExpr::create(ast, $2, $1, $3); LOC($$, @$); }
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

addSubExpr
    : multDivExpr { $$ = $1; }
    | addSubExpr PLUS multDivExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Add, $1, $3); LOC($$, @$); }
    | addSubExpr SUB multDivExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Sub, $1, $3); LOC($$, @$); }
    ;

multDivExpr
    : powerExpr { $$ = $1; }
    | multDivExpr MULT powerExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Mult, $1, $3); LOC($$, @$); }
    | multDivExpr DIV powerExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Div, $1, $3); LOC($$, @$); }
    | multDivExpr MOD powerExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Mod, $1, $3); LOC($$, @$); }
    ;

powerExpr
    : stringExpr { $$ = $1; }
    | powerExpr CARET stringExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Pow, $1, $3); LOC($$, @$); }
    ;

stringExpr
    : unaryAddSubExpr { $$ = $1; }
    | stringExpr stringExpPrefix unaryAddSubExpr { $$ = StringExpr::create(ast, $2, $1, $3); LOC($$, @$); }
    ;

stringExpPrefix
    : STARTS WITH { $$ = StringOperator::StartsWith; }
    | ENDS WITH { $$ = StringOperator::EndsWith; }
    | CONTAINS { $$ = StringOperator::Contains; }
    ;

unaryAddSubExpr
    : atomicExpr { $$ = $1; }
    | PLUS unaryAddSubExpr { $$ = UnaryExpr::create(ast, UnaryOperator::Plus, $2); LOC($$, @$); }
    | SUB unaryAddSubExpr { $$ = UnaryExpr::create(ast, UnaryOperator::Minus, $2); LOC($$, @$); }
    // this allows chaining operators like -+--1 (which is +1)
    ;

atomicExpr
    : propertyOrLabelExpr { $$ = $1; }
    | atomicExpr listExpr { $$ = nullptr; scanner.notImplemented(@$, "List Exprs"); }
    ;

listExpr
    : OBRACK Expr CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK Expr CBRACK"); }
    | OBRACK Expr RANGE Expr CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK Expr RANGE Expr CBRACK"); }
    | OBRACK RANGE Expr CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK RANGE Expr CBRACK"); }
    | OBRACK Expr RANGE CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK Expr RANGE CBRACK"); }
    | OBRACK RANGE CBRACK { $$ = nullptr; scanner.notImplemented(@$, "OBRACK RANGE CBRACK"); }
    ;

propertyOrLabelExpr
    : propertyExpr { $$ = $1; }

    // | propertyExpr nodeLabels
    // This seems too permissive, it allows 'n.name:Person' which is weird

    // Replaced by this more specific rule
    | symbol nodeLabels { $$ = NodeLabelExpr::create(ast, $1, std::move($2)); LOC($$, @$); }
    ;

propertyExpr
    : atomExpr { $$ = $1; }
    | qualifiedName DOT name { $1->addName($3); $$ = PropertyExpr::create(ast, $1); LOC($$, @$); }
    ;

atomExpr
    : pathExpr { $$ = $1; }
    | literal { $$ = LiteralExpr::create(ast, $1); LOC($$, @$); }
    | symbol { $$ = SymbolExpr::create(ast, $1); LOC($$, @$); }

    | parameter { scanner.notImplemented(@$, "Parameters"); }
    | caseExpr { scanner.notImplemented(@$, "CASE"); }
    | countFunc { scanner.notImplemented(@$, "COUNT"); }
    | listComprehension { scanner.notImplemented(@$, "List comprehensions"); }
    //| patternComprehension { scanner.notImplemented(@$, "Pattern comprehensions"); }
    | filterWith { scanner.notImplemented(@$, "Filter keywords"); }
    | functionInvocation { scanner.notImplemented(@$, "Function invocations"); }
    | subqueryExist { scanner.notImplemented(@$, "EXISTS"); }
    | collectExpr
    ;

collectExpr
    : COLLECT OBRACE readingStatements returnSt CBRACE { scanner.notImplemented(@$, "COLLECT"); }
    | COLLECT OPAREN Expr CPAREN { scanner.notImplemented(@$, "COLLECT"); }
    | COLLECT OPAREN DISTINCT Expr CPAREN { scanner.notImplemented(@$, "COLLECT"); }
    ;

patternPart
    : patternElem { $$ = $1; }
    | patternAlias { scanner.notImplemented(@$, "Pattern alias: Symbol = ()-[]-()-[]-()..."); }
    ;

patternAlias
    : symbol ASSIGN patternElem { scanner.notImplemented(@$, "Pattern alias: Symbol = ()-[]-()-[]-()..."); }

patternElem
    : nodePattern { $$ = PatternElement::create(ast); $$->addEntity($1); LOC($$, @$); }
    | patternElem patternElemChain { $$ = $1; $$->addEntity($2.first); $$->addEntity($2.second); }
    ;

patternElemChain
    : edgePattern nodePattern { $$ = std::make_pair($1, $2); }
    ;

properties
    : mapLit { $$ = $1; }
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN {
        $$ = NodePattern::create(ast);
        $$->setSymbol($2);
        if ($3.has_value()) {
            $$->setLabels(std::move($3.value()));
        }
        $$->setProperties($4);
        LOC($$, @$);
    }
    ;

opt_symbol
    : symbol { $$ = $1; }
    | { $$ = nullptr; }
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
    : TAIL_TAIL     { $$ = EdgePattern::create(ast, EdgePattern::Direction::Undirected); LOC($$, @$); } // Undirected
    | TIP_TAIL_TAIL { $$ = EdgePattern::create(ast, EdgePattern::Direction::Backward); LOC($$, @$); } // Directed backwards
    | TAIL_TAIL_TIP { $$ = EdgePattern::create(ast, EdgePattern::Direction::Forward); LOC($$, @$); } // Directed forwards
    | TAIL_BRACKET edgeDetail BRACKET_TAIL     { $$ = $2; $$->setDirection(EdgePattern::Direction::Undirected); LOC($$, @$); } // Undirected
    | TIP_TAIL_BRACKET edgeDetail BRACKET_TAIL { $$ = $2; $$->setDirection(EdgePattern::Direction::Backward); LOC($$, @$); } // Directed backwards
    | TAIL_BRACKET edgeDetail BRACKET_TAIL_TIP { $$ = $2; $$->setDirection(EdgePattern::Direction::Forward); LOC($$, @$); } // Directed forwards
    ;

edgeDetail
    : opt_symbol opt_edgeTypes opt_rangeLit opt_properties { 
        $$ = EdgePattern::create(ast, EdgePattern::Direction::Undirected);
        $$->setSymbol($1);

        if ($2.has_value()) {
            $$->setTypes(std::move($2.value()));
        }

        $$->setProperties($4);
        LOC($$, @$); 
    }
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
    : symbol { $$ = QualifiedName::create(ast); $$->addName($1); }
    | qualifiedName DOT symbol { $$ = $1; $$->addName($3); }
    ;

invocationName
    : qualifiedName { $$ = $1; }
    ;

functionInvocation
    : invocationName OPAREN CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN DISTINCT CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN ExprChain CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    | invocationName OPAREN DISTINCT ExprChain CPAREN { scanner.notImplemented(@$, "Function invocations"); }
    ;

pathExpr
    : parenthesizedExpr { $$ = $1; }
    | OPAREN CPAREN pathExprElem { $$ = PathExpr::create(ast, $3); LOC($$, @$); }
    | OPAREN symbol properties CPAREN pathExprElem { 
        $$ = PathExpr::create(ast, $5);
        NodePattern* nodePattern = NodePattern::create(ast);
        nodePattern->setProperties($3);
        nodePattern->setSymbol($2);
        $5->addRootEntity(nodePattern);
        LOC($$, @$);
    }
    | OPAREN symbol nodeLabels properties CPAREN pathExprElem { 
        $$ = PathExpr::create(ast, $6);
        NodePattern* nodePattern = NodePattern::create(ast);
        nodePattern->setLabels(std::move($3));
        nodePattern->setProperties($4);
        nodePattern->setSymbol($2);
        $6->addRootEntity(nodePattern);
        LOC($$, @$);
    }
    | OPAREN nodeLabels CPAREN pathExprElem {
        $$ = PathExpr::create(ast, $4);
        NodePattern* node = NodePattern::create(ast);
        node->setLabels(std::move($2));
        $4->addRootEntity(node);
        LOC($$, @$);
        LOC(node, @$);
      }
    | OPAREN nodeLabels properties CPAREN pathExprElem {
        $$ = PathExpr::create(ast, $5);
        NodePattern* node = NodePattern::create(ast);
        node->setLabels(std::move($2));
        node->setProperties($3);
        $5->addRootEntity(node);
        LOC($$, @$);
        LOC(node, @$);
      }

    // Those three Exprs are tricky and cause conflicts with 'OPAREN Expr CPAREN'

    //| OPAREN symbol nodeLabels CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized Exprs"); }
    // Causes conflicts because 'symbol nodeLabels' is a valid Expr (propertyOrLabelExpr)

    //| OPAREN symbol CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized Exprs"); }
    // Causes conflicts because 'symbol' is a valid Expr (atomExpr)

    //| OPAREN properties CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized Exprs"); }
    // Causes conflicts because 'properties' is a valid Expr (map literal)

    // Instead, they are handled by the rule below

    | OPAREN Expr CPAREN pathExprElem {
          $$ = PathExpr::create(ast, $4);

          if (NodePattern* nodePattern = NodePattern::fromExpr(ast, $2)) {
              $4->addRootEntity(nodePattern);
          } else {
              error(@1, "Invalid path Expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");
          }

          LOC($$, @$);
      }
    ;

pathExprElem
: patternElemChain { $$ = PatternElement::create(ast); $$->addRootEntity($1.second); $$->addRootEntity($1.first); LOC($$, @$); }
| pathExprElem patternElemChain { $$ = $1; $$->addRootEntity($2.second); $$->addRootEntity($2.first); }
;

parenthesizedExpr
    : OPAREN Expr CPAREN { $$ = $2; }
    ;

filterWith
    : filterKeyword OPAREN filterExpr CPAREN { scanner.notImplemented(@$, "Filters"); }
    ;

filterKeyword
    : ALL
    | ANY
    | NONE
    | SINGLE
    ;

//patternComprehension
//    : OBRACK edgesChainPattern PIPE Expr CBRACK
//    | OBRACK lhs edgesChainPattern PIPE Expr CBRACK
//    | OBRACK edgesChainPattern where PIPE Expr CBRACK
//    | OBRACK lhs edgesChainPattern where PIPE Expr CBRACK
//    ;

//edgesChainPattern
//    : nodePattern patternElemChain
//    | edgesChainPattern patternElemChain
//    ;

listComprehension
    : OBRACK filterExpr CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    | OBRACK filterExpr PIPE Expr CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    ;

filterExpr
    : symbol IN Expr { scanner.notImplemented(@$, "IN"); }
    | symbol IN Expr where { scanner.notImplemented(@$, "IN"); }
    ;

countFunc
    : COUNT OPAREN MULT CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN DISTINCT CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN ExprChain CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OPAREN DISTINCT ExprChain CPAREN { scanner.notImplemented(@$, "COUNT"); }
    | COUNT OBRACE patternWhere CBRACE { scanner.notImplemented(@$, "COUNT"); }

    // Here, returnSt is mandatory for MATCH subqueries, as opposed to Neo4j's Cypher parser
    | COUNT OBRACE query CBRACE { scanner.notImplemented(@$, "COUNT"); }
    ;

caseExpr
    : CASE whenThenChain END { scanner.notImplemented(@$, "CASE"); }
    | CASE Expr whenThenChain END  { scanner.notImplemented(@$, "CASE"); }
    | CASE whenThenChain ELSE Expr END { scanner.notImplemented(@$, "CASE"); }
    | CASE Expr whenThenChain ELSE Expr END { scanner.notImplemented(@$, "CASE"); }
    ;

whenThenChain
    : whenThen
    | whenThenChain whenThen
    ;

whenThen
    : WHEN Expr THEN Expr
    ;

parameter
    : DOLLAR symbol { scanner.notImplemented(@$, "Parameters"); }
    | DOLLAR numLit { scanner.notImplemented(@$, "Parameters"); }
    ;

literal
    : boolLit { $$ = $1; }
    | numLit { $$ = $1; }
    | NULL_ { $$ = NullLiteral::create(ast); }
    | stringLit { $$ = $1; }
    | charLit { $$ = $1; }
    | listLit { scanner.notImplemented(@$, "Lists"); }
    | mapLit { $$ = $1; }
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
    : TRUE { $$ = BoolLiteral::create(ast, true); }
    | FALSE { $$ = BoolLiteral::create(ast, false); }
    ;

numLit
    : DIGIT { $$ = IntegerLiteral::create(ast, $1); }
    | FLOAT { $$ = DoubleLiteral::create(ast, $1); }
    ;

stringLit
    : STRING_LITERAL { $$ = StringLiteral::create(ast, $1); }
    ;

charLit
    : CHAR_LITERAL { $$ = CharLiteral::create(ast, $1); }
    ;

listLit
    : OBRACK CBRACK { scanner.notImplemented(@$, "Lists"); }
    //| OBRACK ExprChain CBRACK // Enabling this causes conflicts
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
    | caseExpr { scanner.notImplemented(@$, "CASE"); }
    | countFunc { scanner.notImplemented(@$, "COUNT"); }
    | listComprehension { scanner.notImplemented(@$, "List comprehensions"); }
    //| patternComprehension { scanner.notImplemented(@$, "Pattern comprehensions"); }
    | filterWith { scanner.notImplemented(@$, "Filters"); }
    | parenthesizedExpr // Enabling this causes conflicts, not needed?
    | functionInvocation { scanner.notImplemented(@$, "Function invocations"); }
    | symbol
    | subqueryExist { scanner.notImplemented(@$, "EXISTS"); }
    ;

mapLit
    : OBRACE CBRACE { $$ = MapLiteral::create(ast); LOC($$, @$); }
    | OBRACE mapPairChain CBRACE { $$ = $2; }
    ;

mapPairChain
    : mapPair { $$ = MapLiteral::create(ast); $$->set($1.first, $1.second); LOC($$, @$); }
    | mapPairChain COMMA mapPair { $$ = $1; $$->set($3.first, $3.second); }
    ;

mapPair
    : name COLON Expr { $$ = std::make_pair($1, $3); }
    ;

name
    : symbol { $$ = $1; }
    | reservedWord { $$ = $1 ; }
    ;

symbol
    : ESC_LITERAL { $$ = Symbol::create(ast, $1); }
    | ID { $$ = Symbol::create(ast, $1); }
    | FILTER { $$ = Symbol::create(ast, $1); }
    | EXTRACT { $$ = Symbol::create(ast, $1); }
    //| ANY { $$ = Symbol::create(ast, $1); } // Causes conflicts
    //| NONE { $$ = Symbol::create(ast, $1); } // Causes conflicts
    //| SINGLE { $$ = Symbol::create(ast, $1); } // Causes conflicts
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
    : ALL { $$ = Symbol::create(ast, $1); }
    | ASC { $$ = Symbol::create(ast, $1); }
    | ASCENDING { $$ = Symbol::create(ast, $1); }
    | BY { $$ = Symbol::create(ast, $1); }
    | CREATE { $$ = Symbol::create(ast, $1); }
    | DELETE { $$ = Symbol::create(ast, $1); }
    | DESC { $$ = Symbol::create(ast, $1); }
    | DESCENDING { $$ = Symbol::create(ast, $1); }
    | DETACH { $$ = Symbol::create(ast, $1); }
    | EXISTS { $$ = Symbol::create(ast, $1); }
    | LIMIT { $$ = Symbol::create(ast, $1); }
    | MATCH { $$ = Symbol::create(ast, $1); }
    | MERGE { $$ = Symbol::create(ast, $1); }
    | ON { $$ = Symbol::create(ast, $1); }
    | IF { $$ = Symbol::create(ast, $1); }
    | OPTIONAL { $$ = Symbol::create(ast, $1); }
    | ORDER { $$ = Symbol::create(ast, $1); }
    | REMOVE { $$ = Symbol::create(ast, $1); }
    | RETURN { $$ = Symbol::create(ast, $1); }
    | SET { $$ = Symbol::create(ast, $1); }
    | SKIP { $$ = Symbol::create(ast, $1); }
    | WHERE { $$ = Symbol::create(ast, $1); }
    | WITH { $$ = Symbol::create(ast, $1); }
    | UNION { $$ = Symbol::create(ast, $1); }
    | UNWIND { $$ = Symbol::create(ast, $1); }
    | AND { $$ = Symbol::create(ast, $1); }
    | AS { $$ = Symbol::create(ast, $1); }
    | CONTAINS { $$ = Symbol::create(ast, $1); }
    | DISTINCT { $$ = Symbol::create(ast, $1); }
    | ENDS { $$ = Symbol::create(ast, $1); }
    | IN { $$ = Symbol::create(ast, $1); }
    | IS { $$ = Symbol::create(ast, $1); }
    | NOT { $$ = Symbol::create(ast, $1); }
    | OR { $$ = Symbol::create(ast, $1); }
    | STARTS { $$ = Symbol::create(ast, $1); }
    | XOR { $$ = Symbol::create(ast, $1); }
    | FALSE { $$ = Symbol::create(ast, $1); }
    | TRUE { $$ = Symbol::create(ast, $1); }
    | NULL_ { $$ = Symbol::create(ast, $1); }
    | CONSTRAINT { $$ = Symbol::create(ast, $1); }
    | DO { $$ = Symbol::create(ast, $1); }
    | FOR { $$ = Symbol::create(ast, $1); }
    | REQUIRE { $$ = Symbol::create(ast, $1); }
    | COLLECT { $$ = Symbol::create(ast, $1); }
    | UNIQUE { $$ = Symbol::create(ast, $1); }
    | CASE { $$ = Symbol::create(ast, $1); }
    | WHEN { $$ = Symbol::create(ast, $1); }
    | THEN { $$ = Symbol::create(ast, $1); }
    | ELSE { $$ = Symbol::create(ast, $1); }
    | END { $$ = Symbol::create(ast, $1); }
    | MANDATORY { $$ = Symbol::create(ast, $1); }
    | SCALAR { $$ = Symbol::create(ast, $1); }
    | OF { $$ = Symbol::create(ast, $1); }
    | ADD { $$ = Symbol::create(ast, $1); }
    | DROP { $$ = Symbol::create(ast, $1); }
    ;

%%

void db::v2::YCypherParser::error(const location_type& l, const std::string& m) {
    scanner.syntaxError(l, m);
}


