%require "3.2"
%language "c++"

%define api.parser.class { YCypherParser }
%define api.value.type variant
%define parse.assert
%define parse.error detailed
%define api.namespace { db }
%parse-param {db::YCypherScanner& scanner}
%parse-param {db::CypherAST* ast}
%define api.location.type { SourceLocation }
%locations

%header
%verbose

%code requires {

    #include <optional>
    #include <string_view>
    #include <utility>
    #include <vector>

    // Inspired by https://github.com/antlr/grammars-v4/blob/master/cypher/CypherParser.g4

    #define LOC(obj, loc) ast->getSourceManager()->setLocation(obj, loc)

    #include "SourceLocation.h"
    #include "SourceManager.h"

    #include "stmt/StmtContainer.h"
    #include "stmt/ReturnStmt.h"
    #include "stmt/MatchStmt.h"
    #include "stmt/ShortestPathStmt.h"
    #include "stmt/CallStmt.h"
    #include "stmt/CreateStmt.h"
    #include "stmt/SetStmt.h"
    #include "stmt/DeleteStmt.h"
    #include "expr/All.h"
    #include "Literal.h"
    #include "Symbol.h"
    #include "SymbolChain.h"
    #include "QuantifiedPath.h"
    #include "FunctionInvocation.h"
    #include "WhereClause.h"
    #include "YieldClause.h"
    #include "YieldItems.h"
    #include "Pattern.h"
    #include "NodePattern.h"
    #include "EdgePattern.h"
    #include "SinglePartQuery.h"
    #include "ChangeQuery.h"
    #include "CommitQuery.h"
    #include "ListGraphQuery.h"
    #include "ListAvailableGraphsQuery.h"
    #include "CreateGraphQuery.h"
    #include "S3ConnectQuery.h"
    #include "S3TransferQuery.h"
    #include "Projection.h"
    #include "PatternElement.h"
    #include "stmt/Skip.h"
    #include "stmt/Limit.h"
    #include "stmt/OrderBy.h"
    #include "stmt/OrderByItem.h"
    #include "stmt/SetItem.h"
    #include "LoadGraphQuery.h"
    #include "LoadGMLQuery.h"
    #include "LoadParquetQuery.h"
    #include "LoadJsonlQuery.h"
    #include "LoadCommitQuery.h"
    #include "ShowProceduresQuery.h"
    #include "ShowExtensionsQuery.h"
    #include "stmt/LoadCSVStmt.h"
    #include "CreateVectorIndexQuery.h"
    #include "LoadVectorQuery.h"
    #include "LoadEmbeddingQuery.h"
    #include "DeleteVectorIndexQuery.h"
    #include "ShowVectorIndexesQuery.h"
    #include "InstallExtensionQuery.h"
    #include "stmt/VectorSearchStmt.h"
    #include "expr/ListExpr.h"
    #include "VecLibMetadata.h"
    #include "CreateNodePropertyIndexQuery.h"
    #include "CreateEdgePropertyIndexQuery.h"
    #include "DropIndexQuery.h"
    #include "MergeDataPartsQuery.h"
    #include "stmt/UnwindStmt.h"
    #include "stmt/WithStmt.h"
    #include "EmbeddingsSpec.h"

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
    #include "ParserException.h"
    #include "ParserUtils.h"
    #include "ExplainRequest.h"

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
%token<std::string_view> PROCEDURES
%token<std::string_view> SHORTESTPATH 
%token<std::string_view> DESCENDING
%token<std::string_view> CONSTRAINT
%token<std::string_view> MANDATORY
%token<std::string_view> ASCENDING
%token<std::string_view> OPTIONAL
%token<std::string_view> CONTAINS
%token<std::string_view> DISTINCT
%token<std::string_view> EXPLAIN
%token<std::string_view> EXTRACT
%token<std::string_view> REQUIRE
%token<std::string_view> COLLECT
%token<std::string_view> CONNECT
%token<std::string_view> SUBMIT
%token<std::string_view> CHANGE
%token<std::string_view> COMMIT
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
%token<std::string_view> MERGE_DATAPARTS
%token<std::string_view> ORDER
%token<std::string_view> WHERE
%token<std::string_view> UNION
%token<std::string_view> FALSE
%token<std::string_view> COUNT
%token<std::string_view> GRAPH
%token<std::string_view> GRAPHS
%token<std::string_view> AVAILABLE
%token<std::string_view> HEADERS
%token<std::string_view> EMBEDDINGS
%token<std::string_view> JSONL
%token<std::string_view> LIST
%token<std::string_view> DESC
%token<std::string_view> CALL
%token<std::string_view> INSTALL
%token<std::string_view> EXTENSIONS
%token<std::string_view> NULL_
%token<std::string_view> TRUE
%token<std::string_view> WHEN
%token<std::string_view> NONE
%token<std::string_view> THEN
%token<std::string_view> ELSE
%token<std::string_view> CASE
%token<std::string_view> ERROR_
%token<std::string_view> ENDS
%token<std::string_view> DROP
%token<std::string_view> SHOW
%token<std::string_view> SKIP
%token<std::string_view> WITH
%token<std::string_view> LOAD
%token<std::string_view> PUSH
%token<std::string_view> PULL
%token<std::string_view> NEW
%token<std::string_view> FAIL
%token<std::string_view> CSV
%token<std::string_view> GML
%token<std::string_view> PARQUET
%token<std::string_view> ANY
%token<std::string_view> SET
%token<std::string_view> ALL
%token<std::string_view> ASC
%token<std::string_view> NOT
%token<std::string_view> END
%token<std::string_view> XOR
%token<std::string_view> FOR
%token<std::string_view> FROM
%token<std::string_view> ADD
%token<std::string_view> AND
%token<std::string_view> OR
%token<std::string_view> IN
%token<std::string_view> IS
%token<std::string_view> S3
%token<std::string_view> BY
%token<std::string_view> DO
%token<std::string_view> OF
%token<std::string_view> ON
%token<std::string_view> IF
%token<std::string_view> AS

// Vector query tokens
%token<std::string_view> VECTOR
%token<std::string_view> EMBEDDING
%token<std::string_view> INDEX
%token<std::string_view> DIMENSION
%token<std::string_view> METRIC
%token<std::string_view> EUCLID
%token<std::string_view> COSINE
%token<std::string_view> HNSW
%token<std::string_view> FLAT
%token<std::string_view> TYPE
%token<std::string_view> INDEXES
%token<std::string_view> SEARCH

%token<std::string_view> ESC_LITERAL
%token<std::string_view> STRING_LITERAL
%token<std::string_view> ID
%token<int64_t> DIGIT
%token<double> DOUBLE

%token UNKNOWN

%type<db::Literal*> numLit literal
%type<db::BoolLiteral*> boolLit
%type<db::StringLiteral*> stringLit
%type<db::MapLiteral*> mapLit
%type<std::pair<db::Symbol*, db::Expr*>> mapPair
%type<db::MapLiteral*> mapPairChain
%type<db::Symbol*> symbol
%type<db::QualifiedName*> qualifiedName
%type<db::QualifiedName*> invocationName
%type<db::Symbol*> name
%type<db::Symbol*> reservedWord
%type<db::FunctionInvocation*> functionInvocation
%type<db::FunctionInvocation*> countFunc

%type<db::SymbolChain*> nodeLabels
%type<db::SymbolChain*> edgeTypes
%type<db::MapLiteral*> properties

%type<db::Symbol*> opt_symbol
%type<db::SymbolChain*> opt_nodeLabels
%type<db::MapLiteral*> opt_properties
%type<db::SymbolChain*> opt_edgeTypes

%type<db::ExprChain*> exprChain
%type<db::ExprChain*> parenExprChain
%type<db::Expr*> expr
%type<db::Expr*> xorExpr
%type<db::Expr*> andExpr
%type<db::Expr*> notExpr
%type<db::Expr*> comparisonExpr
%type<db::Expr*> addSubExpr
%type<db::Expr*> multDivExpr
%type<db::Expr*> powerExpr
%type<db::Expr*> unaryAddSubExpr
%type<db::Expr*> atomicExpr
%type<db::Expr*> stringExpr
%type<db::Expr*> entityTypeExpr
%type<db::Expr*> propertyOrLabelExpr
%type<db::Expr*> propertyExpr
%type<db::Expr*> atomExpr
%type<db::Expr*> collectExpr
%type<db::Expr*> pathExpr
%type<db::Expr*> parenthesizedExpr

%type<db::Expr*> projectionItem
%type<db::Projection*> projectionItems
%type<db::Projection*> projectionBody

%type<db::BinaryOperator> comparisonSign
%type<db::StringOperator> stringExpPrefix

%type<db::Pattern*> pattern
%type<db::Pattern*> patternWhere
%type<db::PatternElement*> patternPart
%type<db::PatternElement*> patternElem
%type<db::PatternElement*> pathExprElem
%type<db::NodePattern*> nodePattern
%type<db::EdgePattern*> edgePattern
%type<db::EdgePattern*> edgeDetail
%type<std::pair<db::EdgePattern*, db::NodePattern*>> patternElemChain
%type<db::WhereClause*> whereClause
%type<db::WhereClause*> opt_whereClause
%type<db::YieldClause*> yieldClause
%type<db::YieldItems*> yieldItemChain
%type<db::YieldItems*> yieldItems
%type<db::SymbolExpr*> yieldItem
%type<db::QuantifiedPath*> quantifiedPath
%type<db::QuantifiedPath*> opt_quantifiedPath
%type<db::QuantifiedPath*> quantifiedPathRange

%type<db::SinglePartQuery*> singlePartQuery
%type<db::ChangeQuery*> changeQuery
%type<db::CommitQuery*> commitQuery
%type<db::MergeDataPartsQuery*> mergeDataPartsQuery
%type<db::ListGraphQuery*> listGraphQuery
%type<db::ListAvailableGraphsQuery*> listAvailableGraphsQuery
%type<db::CreateGraphQuery*> createGraphQuery
%type<db::InstallExtensionQuery*> installExtensionQuery
%type<db::S3ConnectQuery*> s3ConnectQuery
%type<db::S3TransferQuery*> s3TransferQuery
%type<db::ShowProceduresQuery*> showProceduresQuery
%type<db::ShowExtensionsQuery*> showExtensionsQuery
%type<db::CreateVectorIndexQuery*> createVectorIndexQuery
%type<db::LoadVectorQuery*> loadVectorQuery
%type<db::LoadEmbeddingQuery*> loadEmbedding
%type<db::DeleteVectorIndexQuery*> deleteVectorIndexQuery
%type<db::ShowVectorIndexesQuery*> showVectorIndexesQuery
%type<db::VectorSearchStmt*> vectorSearchSt
%type<vec::DistanceMetric> distanceMetric
%type<vec::IndexType> indexType
%type<db::EmbeddingLiteral*> embeddingLit
%type<db::EmbeddingLiteral*> embeddingLitItems
%type<double> embeddingLitItem
%type<db::ListLiteral*> listLit
%type<db::ListLiteral*> listLitItems
%type<db::Expr*> listLitItem
%type<db::QueryCommand*> singleQuery
%type<db::QueryCommand*> query
%type<db::QueryCommand*> explainQuery
%type<std::vector<std::string_view>> explainPassNames
%type<db::LoadGraphQuery*> loadGraph
%type<db::LoadGMLQuery*> loadGML
%type<db::LoadParquetQuery*> loadParquet
%type<db::LoadJsonlQuery*> loadJsonl
%type<db::LoadCommitQuery*> loadCommitQuery
%type<db::Stmt*> readingStatement
%type<db::Stmt*> updatingStatement
%type<db::StmtContainer*> readingStatements
%type<db::StmtContainer*> updatingStatements
%type<db::ChangeOp> changeOp
%type<db::MatchStmt*> matchSt
%type<db::WithStmt*> withSt
%type<db::ShortestPathStmt*> shortestPathSt
%type<db::CallStmt*> callSt
%type<db::CreateStmt*> createSt
%type<db::SetStmt*> setSt
%type<db::SetItem*> setItem
%type<db::LoadCSVStmt*> loadCSVSt
%type<db::DeleteStmt*> deleteSt
%type<db::ReturnStmt*> returnSt
%type<db::Skip*> skipSSt
%type<db::Limit*> limitSSt
%type<db::OrderBy*> orderBySSt
%type<db::Skip*> opt_skipSSt
%type<db::Limit*> opt_limitSSt
%type<db::OrderBy*> opt_orderBySSt
%type<db::OrderByItem*> orderByItem
%type<bool> opt_distinct
%type<db::CreateNodePropertyIndexQuery*> createNodePropertyIndexQuery
%type<db::CreateEdgePropertyIndexQuery*> createEdgePropertyIndexQuery
%type<db::DropIndexQuery*> dropIndexQuery
%type<db::UnwindStmt*> unwindSt
%type<EmbeddingsSpec> embeddingSpecs
%type<std::pair<std::string_view, size_t>> embeddingSpec

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
    | explainQuery
    ;

explainQuery
    : EXPLAIN singleQuery {
        ast->explainRequest().requestDefaults();
        $$ = $2;
      }
    | EXPLAIN OPAREN explainOptions CPAREN singleQuery { $$ = $5; }
    ;

explainOptions
    : explainOption
    | explainOptions COMMA explainOption
    ;

explainOption
    : ID {
        if (!ast->explainRequest().requestStage($1)) {
            std::string message;
            ExplainRequest::describeOptions($1, message);
            scanner.syntaxError(@1, message);
        }
      }
    | ALL { ast->explainRequest().requestAll(); }
    | ID ID {
        if (!ast->explainRequest().requestPass($1, $2)) {
            std::string message;
            ExplainRequest::describeOptions($1, message);
            scanner.syntaxError(@1, message);
        }
      }
    | ID OBRACK explainPassNames CBRACK {
        ExplainRequest& explain = ast->explainRequest();

        for (const std::string_view passName : $3) {
            if (!explain.requestPass($1, passName)) {
                std::string message;
                ExplainRequest::describeOptions($1, message);
                scanner.syntaxError(@1, message);
            }
        }
      }
    ;

explainPassNames
    : ID {
        $$.push_back($1);
      }
    | explainPassNames COMMA ID {
        $$ = std::move($1);
        $$.push_back($3);
      }
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
    | loadGraph { $$ = $1; }
    | changeQuery { $$ = $1; }
    | commitQuery { $$ = $1; }
    | listGraphQuery { $$ = $1; }
    | listAvailableGraphsQuery { $$ = $1; }
    | createGraphQuery { $$ = $1; }
    | loadGML { $$ = $1; }
    | loadParquet { $$ = $1; }
    | loadJsonl { $$ = $1; }
    | loadCommitQuery { $$ = $1; }
    | s3ConnectQuery { $$ = $1; }
    | s3TransferQuery { $$ = $1; }
    | showProceduresQuery { $$ = $1; }
    | createVectorIndexQuery { $$ = $1; }
    | createNodePropertyIndexQuery { $$ = $1; }
    | createEdgePropertyIndexQuery { $$ = $1; }
    | dropIndexQuery { $$ = $1; }
    | loadVectorQuery { $$ = $1; }
    | loadEmbedding { $$ = $1; }
    | deleteVectorIndexQuery { $$ = $1; }
    | showVectorIndexesQuery { $$ = $1; }
    | installExtensionQuery { $$ = $1; }
    | showExtensionsQuery { $$ = $1; }
    | mergeDataPartsQuery { $$ = $1; }
    ;

loadGraph
    : LOAD GRAPH ID { $$ = LoadGraphQuery::create(ast, $3); LOC($$, @$); }
    ;

loadJsonl
    : LOAD JSONL STRING_LITERAL { $$ = LoadJsonlQuery::create(ast, fs::Path(std::string($3))); LOC($$, @$); }
    | LOAD JSONL STRING_LITERAL AS ID {
        $$ = LoadJsonlQuery::create(ast, fs::Path(std::string($3)));
        $$->setGraphName($5);
        LOC($$, @$);
      }
    | LOAD JSONL STRING_LITERAL WITH EMBEDDINGS OBRACK embeddingSpecs CBRACK {
        $$ = LoadJsonlQuery::create(ast, fs::Path(std::string($3)));
        $$->setEmbeddingSpecs(std::move($7));
        LOC($$, @$);
      }
    | LOAD JSONL STRING_LITERAL AS ID WITH EMBEDDINGS OBRACK embeddingSpecs CBRACK {
        $$ = LoadJsonlQuery::create(ast, fs::Path(std::string($3)));
        $$->setGraphName($5);
        $$->setEmbeddingSpecs(std::move($9));
        LOC($$, @$);
      }
    ;

embeddingSpecs
    : embeddingSpec {
        $$.emplace($1.first, $1.second);
      }
    | embeddingSpecs COMMA embeddingSpec {
        $$ = std::move($1);
        $$.emplace($3.first, $3.second);
      }
    ;

embeddingSpec
    : OBRACE STRING_LITERAL COMMA DIGIT CBRACE {
        $$ = {$2, static_cast<size_t>($4)};
      }
    | OBRACE ID COMMA DIGIT CBRACE {
        $$ = {$2, static_cast<size_t>($4)};
      }
    ;

listGraphQuery
    : LIST GRAPH { $$ = ListGraphQuery::create(ast); LOC($$, @$); }
    ;

listAvailableGraphsQuery
    : LIST AVAILABLE GRAPHS { $$ = ListAvailableGraphsQuery::create(ast); LOC($$, @$); }
    ;

showProceduresQuery
    : SHOW PROCEDURES { $$ = ShowProceduresQuery::create(ast); LOC($$, @$); }
    ;

showExtensionsQuery
    : SHOW EXTENSIONS { $$ = ShowExtensionsQuery::create(ast); LOC($$, @$); }
    ;

installExtensionQuery
    : INSTALL ID { $$ = InstallExtensionQuery::create(ast, $2); LOC($$, @$); }
    ;

// CREATE VECTOR INDEX vector1 WITH DIMENSION 4 METRIC EUCLID [TYPE HNSW|FLAT]
createVectorIndexQuery
    // no type provided => default to flat
    : CREATE VECTOR INDEX ID WITH DIMENSION DIGIT METRIC distanceMetric {
        $$ = CreateVectorIndexQuery::create(ast, $4, static_cast<uint64_t>($7), $9, vec::IndexType::FLAT);
        LOC($$, @$);
      }
    | CREATE VECTOR INDEX ID WITH DIMENSION DIGIT METRIC distanceMetric TYPE indexType {
        $$ = CreateVectorIndexQuery::create(ast, $4, static_cast<uint64_t>($7), $9, $11);
        LOC($$, @$);
      }
    ;

indexType
    : HNSW { $$ = vec::IndexType::HNSW; }
    | FLAT { $$ = vec::IndexType::FLAT; }
    ;

createNodePropertyIndexQuery
    : CREATE INDEX ID FOR nodePattern ON propertyExpr {
        $$ = CreateNodePropertyIndexQuery::create(ast, $3, $5, dynamic_cast<PropertyExpr*>($7));
        LOC($$, @$);
      }
    ;

createEdgePropertyIndexQuery
    : CREATE INDEX ID FOR OBRACK edgeDetail CBRACK ON propertyExpr {
        $$ = CreateEdgePropertyIndexQuery::create(ast, $3, $6, dynamic_cast<PropertyExpr*>($9));
        LOC($$, @$);
      }
    ;

dropIndexQuery
    : DROP INDEX ID { $$ = DropIndexQuery::create(ast, $3); LOC($$, @$); }

distanceMetric
    : EUCLID { $$ = vec::DistanceMetric::EUCLIDEAN_DIST; }
    | COSINE { $$ = vec::DistanceMetric::INNER_PRODUCT; }
    ;

// LOAD VECTOR FROM "filepath" IN vector1
loadVectorQuery
    : LOAD VECTOR FROM STRING_LITERAL IN ID {
        $$ = LoadVectorQuery::create(ast, $4, $6);
        LOC($$, @$);
      }
    ;

// LOAD EMBEDDING FROM "filepath" AS embeddingProperty
loadEmbedding
    : LOAD EMBEDDING FROM STRING_LITERAL AS ID {
        $$ = LoadEmbeddingQuery::create(ast, $4, $6);
        LOC($$, @$);
      }
    ;

// DELETE VECTOR INDEX vector1
deleteVectorIndexQuery
    : DELETE VECTOR INDEX ID {
        $$ = DeleteVectorIndexQuery::create(ast, $4);
        LOC($$, @$);
      }
    ;

// SHOW VECTOR INDEXES
showVectorIndexesQuery
    : SHOW VECTOR INDEXES { $$ = ShowVectorIndexesQuery::create(ast); LOC($$, @$); }
    ;

// VECTOR SEARCH IN vector1 FOR 10 [0.1, 0.2, ...] YIELD ids
vectorSearchSt
    : VECTOR SEARCH IN ID FOR DIGIT embeddingLit yieldClause {
        $$ = VectorSearchStmt::create(ast);
        $$->setIndexName($4);
        $$->setK(static_cast<uint64_t>($6));
        $$->setQueryVector($7);
        $$->setYield($8);
        LOC($$, @$);
      }

loadCommitQuery
    : LOAD COMMIT STRING_LITERAL { $$ = LoadCommitQuery::create(ast, $3); LOC($$, @$);}
    ;

createGraphQuery
    : CREATE GRAPH ID { $$ = CreateGraphQuery::create(ast, $3); LOC($$, @$); }
    ;

loadGML
    : LOAD GML STRING_LITERAL AS ID {
        $$ = LoadGMLQuery::create(ast, fs::Path(std::string($3)));
        $$->setGraphName($5);
        LOC($$, @$);
      }
    | LOAD GML STRING_LITERAL {
        $$ = LoadGMLQuery::create(ast, fs::Path(std::string($3)));
        LOC($$, @$);
      }
    ;

loadParquet
    : LOAD PARQUET STRING_LITERAL AS ID {
        $$ = LoadParquetQuery::create(ast, fs::Path(std::string($3)));
        $$->setGraphName($5);
        LOC($$, @$);
      }
    | LOAD PARQUET STRING_LITERAL {
        $$ = LoadParquetQuery::create(ast, fs::Path(std::string($3)));
        LOC($$, @$);
      }
    ;

s3ConnectQuery
    : S3 CONNECT STRING_LITERAL STRING_LITERAL STRING_LITERAL {
        $$ = S3ConnectQuery::create(ast, $3, $4, $5);
        LOC($$, @$);
      }
    ;

s3TransferQuery
//             <S3-URL>       <LOCAL-DIR>
    : S3 PULL STRING_LITERAL STRING_LITERAL { $$ = S3TransferQuery::create(ast,
                                                   S3TransferQuery::Direction::PULL,
                                                   $3,
                                                   $4); LOC($$, @$); }
//            <LOCAL-DIR>     <S3-URL>
    | S3 PUSH STRING_LITERAL STRING_LITERAL { $$ = S3TransferQuery::create(ast,
                                                   S3TransferQuery::Direction::PUSH,
                                                   $4,
                                                   $3); LOC($$, @$); }
    ;

returnSt
    : RETURN projectionBody { $$ = ReturnStmt::create(ast, $2); LOC($$, @$); }
    ;

withSt
    : WITH projectionBody opt_whereClause {
        $$ = WithStmt::create(ast, $2);
        $$->setWhere($3);
        LOC($$, @$);
      }
    ;

opt_whereClause
    : whereClause { $$ = $1; }
    | { $$ = nullptr; }
    ;

skipSSt
    : SKIP expr { $$ = Skip::create(ast, $2); LOC($$, @$); }
    ;

limitSSt
    : LIMIT expr { $$ = Limit::create(ast, $2); LOC($$, @$); }
    ;

projectionBody
    : opt_distinct projectionItems opt_orderBySSt opt_skipSSt opt_limitSSt {
        $$ = $2;
        $$->setDistinct($1);
        $$->setOrderBy($3);
        $$->setSkip($4);
        $$->setLimit($5);
      }
    ;

opt_distinct
    : DISTINCT { $$ = true; }
    | { $$ = false; }
    ;

opt_orderBySSt
    : orderBySSt { $$ = $1; }
    | { $$ = nullptr; }
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
    : MULT { $$ = Projection::create(ast); $$->setReturnAll(); LOC($$, @$); } // wildcard
    | projectionItem { $$ = Projection::create(ast); $$->addExpr($1); LOC($$, @$); }
    | projectionItems COMMA projectionItem { $$ = $1; $$->addExpr($3); LOC($$, @$); }
    ;

projectionItem
    : expr { $$ = $1; LOC($$, @$); }
    | expr AS name { $$ = $1; $$->setName($3->getName()); LOC($$, @$); }
    ;

orderByItem
    : expr { $$ = OrderByItem::create(ast, $1); LOC($$, @$); }
    | expr ASCENDING { $$ = OrderByItem::create(ast, $1, OrderByType::ASC); LOC($$, @$); }
    | expr ASC { $$ = OrderByItem::create(ast, $1, OrderByType::ASC); LOC($$, @$); }
    | expr DESCENDING { $$ = OrderByItem::create(ast, $1, OrderByType::DESC); LOC($$, @$); }
    | expr DESC { $$ = OrderByItem::create(ast, $1, OrderByType::DESC); LOC($$, @$); }
    ;

orderBySSt
    : ORDER BY orderByItem { $$ = OrderBy::create(ast); $$->addItem($3); LOC($$, @$); }
    | orderBySSt COMMA orderByItem { $$ = $1; $$->addItem($3); LOC($$, @$); }
    ;

singlePartQuery
    : returnSt {
        $$ = SinglePartQuery::create(ast);
        $$->setReturnStmt($1);
        LOC($$, @$);
      }
    | callSt {
        $$ = SinglePartQuery::create(ast);
        auto* stmts = StmtContainer::create(ast);
        $1->setStandaloneCall(true);
        stmts->add($1);
        $$->setReadStmts(stmts);
        LOC($$, @$);
      }
    | updatingStatements { $$ = SinglePartQuery::create(ast); $$->setUpdateStmts($1); LOC($$, @$); }
    | updatingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setUpdateStmts($1); $$->setReturnStmt($2); LOC($$, @$); }
    | readingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setReturnStmt($2); LOC($$, @$); }
    | readingStatements updatingStatements { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setUpdateStmts($2); LOC($$, @$); }
    | readingStatements updatingStatements returnSt { $$ = SinglePartQuery::create(ast); $$->setReadStmts($1); $$->setUpdateStmts($2); $$->setReturnStmt($3); LOC($$, @$); }
    | readingStatements shortestPathSt returnSt {
        $$ = SinglePartQuery::create(ast);
        $$->setReadStmts($1);
        $$->addReadStmt($2);
        $$->setReturnStmt($3);
        LOC($$, @$); }
    ;

changeQuery
    : CHANGE changeOp { $$ = ChangeQuery::create(ast, $2); LOC($$, @$); }
    ;

changeOp
    : NEW { $$ = ChangeOp::NEW; }
    | SUBMIT { $$ = ChangeOp::SUBMIT; }
    | DELETE { $$ = ChangeOp::DELETE; }
    | LIST { $$ = ChangeOp::LIST; }
    ;

commitQuery
    : COMMIT { $$ = CommitQuery::create(ast); LOC($$, @$); }
    ;

mergeDataPartsQuery
    : MERGE_DATAPARTS { $$ = MergeDataPartsQuery::create(ast); LOC($$, @$); }
    ;

readingStatements
    : readingStatement { $$ = StmtContainer::create(ast); $$->add($1); LOC($$, @$); }
    | readingStatements readingStatement { $$ = $1; $$->add($2); LOC($$, @$); }
    ;

updatingStatements
    : updatingStatement { $$ = StmtContainer::create(ast); $$->add($1); LOC($$, @$); }
    | updatingStatements updatingStatement { $$ = $1; $$->add($2); LOC($$, @$); }
    ;
 
multiPartQuery
    : updateWithSt singlePartQuery { scanner.notImplemented(@$, "Update + With + Reading"); }
    | readingStatements updateWithSt singlePartQuery { scanner.notImplemented(@$, "Reading + With + Update + Reading"); }
    ;

updateWithSt
    : updatingStatements withSt { scanner.notImplemented(@$, "Update + With"); }
    | updateWithSt updatingStatements withSt { scanner.notImplemented(@$, "Update + With + Update"); }
    ;

matchSt
    : MATCH patternWhere opt_orderBySSt opt_skipSSt opt_limitSSt {
        $$ = MatchStmt::create(ast, $2);
        $$->setOrderBy($3);
        $$->setSkip($4);
        $$->setLimit($5);
        LOC($$, @$);
      }
    | OPTIONAL MATCH patternWhere opt_orderBySSt opt_skipSSt opt_limitSSt {
        $$ = MatchStmt::create(ast, $3);
        $$->setOrderBy($4);
        $$->setSkip($5);
        $$->setLimit($6);
        $$->setOptional(true);
        LOC($$, @$);
      }
    ;

shortestPathSt
    : SHORTESTPATH OPAREN symbol COMMA symbol COMMA name COMMA symbol COMMA symbol CPAREN {
        $$ = ShortestPathStmt::create(ast,$3,$5,$7,$9,$11);
        LOC($$, @$);
      }
    ;

unwindSt
    : UNWIND expr AS symbol { $$ = UnwindStmt::create(ast, $2, $4); LOC($$, @$); }
    ;

loadCSVSt
    : LOAD CSV STRING_LITERAL AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $5); LOC($$, @$);
      }
    | LOAD CSV STRING_LITERAL WITH HEADERS AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $7);
        $$->setHasHeaders(true); LOC($$, @$);
      }
    | LOAD CSV STRING_LITERAL ON ERROR_ SKIP AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $8);
        $$->setSkipOnError(true); LOC($$, @$);
      }
    | LOAD CSV STRING_LITERAL WITH HEADERS ON ERROR_ SKIP AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $10);
        $$->setHasHeaders(true);
        $$->setSkipOnError(true); LOC($$, @$);
      }
    | LOAD CSV STRING_LITERAL ON ERROR_ FAIL AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $8);
        LOC($$, @$);
      }
    | LOAD CSV STRING_LITERAL WITH HEADERS ON ERROR_ FAIL AS symbol {
        $$ = LoadCSVStmt::create(ast, $3, $10);
        $$->setHasHeaders(true);
        LOC($$, @$);
      }
    ;

readingStatement
    : matchSt { $$ = $1; }
    | withSt { $$ = $1; }
    | unwindSt { $$ = $1; }
    | callSt { $$ = $1; }
    | loadCSVSt { $$ = $1; }
    | vectorSearchSt { $$ = $1; }
    ;

updatingStatement
    : createSt { $$ = $1; }
    | mergeSt { scanner.notImplemented(@$, "MERGE"); }
    | deleteSt { $$ = $1; }
    | setSt { $$ = $1; }
    | removeSt { scanner.notImplemented(@$, "REMOVE"); }
    ;
 
deleteSt
    : DELETE exprChain { $$ = DeleteStmt::create(ast, $2); LOC($$, @$); }
    | DETACH DELETE exprChain { $$ = DeleteStmt::create(ast, $3); $$->setDetaching(true); LOC($$, @$); }
    ;

removeSt
    : REMOVE removeItemChain { scanner.notImplemented(@$, "REMOVE"); }
    ;

removeItemChain
    : removeItem { scanner.notImplemented(@$, "REMOVE"); }
    | removeItemChain COMMA removeItem { scanner.notImplemented(@$, "REMOVE multiple items"); }
    ;

removeItem
    : entityTypeExpr { scanner.notImplemented(@$, "REMOVE"); }
    | propertyExpr { scanner.notImplemented(@$, "REMOVE"); }
    ;

callSt
    : CALL invocationName parenExprChain {
        $$ = CallStmt::create(ast);
        auto* func = FunctionInvocation::create(ast, $2);
        func->setArguments($3);
        auto* expr = FunctionInvocationExpr::create(ast, func);
        $$->setFunc(expr);
        LOC($$, @$);
    }
    | CALL invocationName parenExprChain yieldClause {
        $$ = CallStmt::create(ast);
        auto* func = FunctionInvocation::create(ast, $2);
        func->setArguments($3);
        auto* expr = FunctionInvocationExpr::create(ast, func);
        $$->setFunc(expr);
        $$->setYield($4);
        LOC($$, @$);
    }
    | OPTIONAL CALL invocationName parenExprChain {
        $$ = CallStmt::create(ast);
        auto* func = FunctionInvocation::create(ast, $3);
        func->setArguments($4);
        auto* expr = FunctionInvocationExpr::create(ast, func);
        $$->setFunc(expr);
        $$->setOptional(true);
        LOC($$, @$);
    }
    | OPTIONAL CALL invocationName parenExprChain yieldClause {
        $$ = CallStmt::create(ast);
        auto* func = FunctionInvocation::create(ast, $3);
        func->setArguments($4);
        auto* expr = FunctionInvocationExpr::create(ast, func);
        $$->setFunc(expr);
        $$->setOptional(true);
        $$->setYield($5);
        LOC($$, @$);
    }
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

exprChain
    : expr { $$ = ExprChain::create(ast); $$->add($1); LOC($$, @$); }
    | exprChain COMMA expr { $$ = $1; $$->add($3); LOC($$, @$); }
    ;

yieldClause
    : YIELD yieldItems { $$ = YieldClause::create(ast); $$->setItems($2); LOC($$, @$); }
    | YIELD MULT { $$ = YieldClause::create(ast); LOC($$, @$); }
    ;

parenExprChain
    : OPAREN exprChain CPAREN { $$ = $2; LOC($$, @$); }
    | OPAREN CPAREN { $$ = ExprChain::create(ast); LOC($$, @$); }
    ;

yieldItems
    : yieldItemChain { $$ = $1; LOC($$, @$); }
    | yieldItemChain whereClause { $$ = $1; $$->setWhere($2); LOC($$, @$); }
    ;

yieldItemChain
    : yieldItem { $$ = YieldItems::create(ast); $$->add($1); LOC($$, @$); }
    | yieldItemChain COMMA yieldItem { $$ = $1; $$->add($3); LOC($$, @$); }
    ;

yieldItem
    : name { $$ = SymbolExpr::create(ast, $1); LOC($$, @$); }
    | name AS name {
        $1->setAs($3->getName());
        $$ = SymbolExpr::create(ast, $1);
        LOC($$, @$);
      }
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
    : SET setItem { $$ = SetStmt::create(ast); $$->addItem($2); LOC($$, @$); }
    | setSt COMMA setItem { $$ = $1; $$->addItem($3); }
    ;

setItem
    : propertyExpr ASSIGN expr { $$ = SetItem::create(ast, static_cast<PropertyExpr*>($1), $3); LOC($$, @$); }
    | symbol ADD_ASSIGN expr { $$ = SetItem::create(ast, $1, $3); LOC($$, @$); }
    | entityTypeExpr { $$ = SetItem::create(ast, static_cast<EntityTypeExpr*>($1)); LOC($$, @$); }
    ;

nodeLabels
    : COLON name { $$ = SymbolChain::create(ast); $$->add($2); }
    | COLON parameter { scanner.notImplemented(@$, "Parameters"); }
    | nodeLabels COLON name { $$ = $1; $$->add($3); }
    | nodeLabels COLON parameter { $$ = $1; scanner.notImplemented(@$, "Parameters"); }
    ;

createSt
    : CREATE pattern { $$ = CreateStmt::create(ast, $2); LOC($$, @$); }
    ;

patternWhere
    : pattern { $$ = $1; LOC($$, @$); }
    | pattern whereClause { $1->setWhere($2); $$ = $1; LOC($$, @$); }
    ;

whereClause
    : WHERE expr { $$ = WhereClause::create(ast, $2); LOC($$, @$); }
    ;

pattern
    : patternPart { $$ = Pattern::create(ast); $$->addElement($1); LOC($$, @$); }
    | pattern COMMA patternPart { $$ = $1, $$->addElement($3); LOC($$, @$); }
    ;

expr
    : xorExpr { $$ = $1; }
    | expr OR xorExpr { $$ = BinaryExpr::create(ast, BinaryOperator::Or, $1, $3); LOC($$, @$); }
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
    | comparisonExpr comparisonSign addSubExpr {
        $$ = BinaryExpr::create(ast, $2, $1, $3);
        LOC($$, @$);
      }
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
    | addSubExpr PLUS multDivExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Add, $1, $3);
        LOC($$, @$);
      }
    | addSubExpr SUB multDivExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Sub, $1, $3);
        LOC($$, @$);
      }
    ;

multDivExpr
    : powerExpr { $$ = $1; }
    | multDivExpr MULT powerExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Mult, $1, $3);
        LOC($$, @$);
      }
    | multDivExpr DIV powerExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Div, $1, $3);
        LOC($$, @$);
      }
    | multDivExpr MOD powerExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Mod, $1, $3);
        LOC($$, @$);
      }
    ;

powerExpr
    : stringExpr { $$ = $1; }
    | powerExpr CARET stringExpr {
        $$ = BinaryExpr::create(ast, BinaryOperator::Pow, $1, $3);
        LOC($$, @$);
      }
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
    | atomExpr { $$ = $1; }
    | atomicExpr OBRACK expr CBRACK {
        $$ = IndexExpr::create(ast, $1, $3); LOC($$, @$);
      }
    ;

propertyOrLabelExpr
    : propertyExpr { $$ = $1; }

    // | propertyExpr nodeLabels
    // This seems too permissive, it allows 'n.name:Person' which is weird

    // Replaced by this more specific rule
    | entityTypeExpr { $$ = $1; }
    ;

entityTypeExpr
    : symbol nodeLabels { $$ = EntityTypeExpr::create(ast, $1, $2); LOC($$, @$); }
    ;

propertyExpr
    : qualifiedName DOT name { $1->addName($3); $$ = PropertyExpr::create(ast, $1); LOC($$, @$); }
    ;

atomExpr
    : pathExpr { $$ = $1; }
    | literal { $$ = LiteralExpr::create(ast, $1); LOC($$, @$); }
    | symbol { $$ = SymbolExpr::create(ast, $1); LOC($$, @$); }

    | parameter { scanner.notImplemented(@$, "Parameters"); }
    | caseExpr { scanner.notImplemented(@$, "CASE"); }
    | countFunc { $$ = FunctionInvocationExpr::create(ast, $1); LOC($$, @$); }
    | listComprehension { scanner.notImplemented(@$, "List comprehensions"); }
    //| patternComprehension { scanner.notImplemented(@$, "Pattern comprehensions"); }
    | filterWith { scanner.notImplemented(@$, "Filter keywords"); }
    | functionInvocation { $$ = FunctionInvocationExpr::create(ast, $1); LOC($$, @$); }
    | subqueryExist { scanner.notImplemented(@$, "EXISTS"); }
    | collectExpr
    ;

collectExpr
    : COLLECT OBRACE readingStatements returnSt CBRACE { scanner.notImplemented(@$, "COLLECT"); }
    | COLLECT OPAREN expr CPAREN {
        Symbol* symbol = Symbol::create(ast, "collect");
        QualifiedName* name = QualifiedName::create(ast);
        name->addName(symbol);
        FunctionInvocation* invocation = FunctionInvocation::create(ast, name);
        ExprChain* arguments = ExprChain::create(ast);
        arguments->add($3);
        invocation->setArguments(arguments);
        $$ = FunctionInvocationExpr::create(ast, invocation);
        LOC($$, @$);
      }
    | COLLECT OPAREN DISTINCT expr CPAREN {
        Symbol* symbol = Symbol::create(ast, "collect");
        QualifiedName* name = QualifiedName::create(ast);
        name->addName(symbol);
        FunctionInvocation* invocation = FunctionInvocation::create(ast, name);
        ExprChain* arguments = ExprChain::create(ast);
        arguments->add($4);
        invocation->setArguments(arguments);
        invocation->setDistinct(true);
        $$ = FunctionInvocationExpr::create(ast, invocation);
        LOC($$, @$);
      }
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
    : edgePattern opt_quantifiedPath nodePattern {
        if ($2) $1->setQuantifiedPath($2);
        $$ = std::make_pair($1, $3);
    }
    ;

opt_quantifiedPath
    : /* empty */ { $$ = nullptr; }
    | quantifiedPath { $$ = $1; LOC($$, @$); }
    ;

quantifiedPath
    : PLUS { $$ = QuantifiedPath::create(ast); $$->setLhs(1); LOC($$, @$); }
    | MULT { $$ = QuantifiedPath::create(ast); LOC($$, @$); }
    | quantifiedPathRange { $$ = $1; }
    ;

quantifiedPathRange
    : OBRACE COMMA CBRACE { $$ = QuantifiedPath::create(ast); LOC($$, @$); }
    | OBRACE DIGIT CBRACE { $$ = QuantifiedPath::create(ast); $$->setLhs($2); $$->setRhs($2); LOC($$, @$); }
    | OBRACE DIGIT COMMA CBRACE { $$ = QuantifiedPath::create(ast); $$->setLhs($2); LOC($$, @$); }
    | OBRACE COMMA DIGIT CBRACE { $$ = QuantifiedPath::create(ast); $$->setRhs($3); LOC($$, @$); }
    | OBRACE DIGIT COMMA DIGIT CBRACE { $$ = QuantifiedPath::create(ast); $$->setLhs($2); $$->setRhs($4); LOC($$, @$); }
    ;

properties
    : mapLit { $$ = $1; }
    ;

nodePattern
    : OPAREN opt_symbol opt_nodeLabels opt_properties CPAREN {
        $$ = NodePattern::create(ast);
        $$->setSymbol($2);
        $$->setLabels($3);
        $$->setProperties($4);
        LOC($$, @$);
      }
    ;

opt_symbol
    : symbol { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_nodeLabels
    : nodeLabels { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_properties
    : properties { $$ = $1; }
    | { $$ = nullptr; }
    ;

opt_edgeTypes
    : edgeTypes { $$ = $1; }
    | { $$ = nullptr; }
    ;

//lhs
//    : symbol ASSIGN
//    ;


edgePattern
    : TAIL_TAIL     { $$ = EdgePattern::create(ast, nullptr, EdgePattern::Direction::Undirected); LOC($$, @$); } // Undirected
    | TIP_TAIL_TAIL { $$ = EdgePattern::create(ast, nullptr, EdgePattern::Direction::Backward); LOC($$, @$); } // Directed backwards
    | TAIL_TAIL_TIP { $$ = EdgePattern::create(ast, nullptr, EdgePattern::Direction::Forward); LOC($$, @$); } // Directed forwards
    | TAIL_BRACKET edgeDetail BRACKET_TAIL     { $$ = $2; $$->setDirection(EdgePattern::Direction::Undirected); LOC($$, @$); } // Undirected
    | TIP_TAIL_BRACKET edgeDetail BRACKET_TAIL { $$ = $2; $$->setDirection(EdgePattern::Direction::Backward); LOC($$, @$); } // Directed backwards
    | TAIL_BRACKET edgeDetail BRACKET_TAIL_TIP { $$ = $2; $$->setDirection(EdgePattern::Direction::Forward); LOC($$, @$); } // Directed forwards
    ;

edgeDetail
    : opt_symbol opt_edgeTypes opt_properties { 
        $$ = EdgePattern::create(ast, nullptr, EdgePattern::Direction::Undirected);
        $$->setSymbol($1);
        $$->setTypes($2);
        $$->setProperties($3);
        LOC($$, @$); 
      }
    ;

edgeTypes
    : COLON name { $$ = SymbolChain::create(ast); $$->add($2); }
    // TODO: Enable the below two cases once we move from v2 -> MLIR
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
    | qualifiedName DOT name { $$ = $1; $$->addName($3); }
    ;

invocationName
    : qualifiedName { $$ = $1; }
    ;

functionInvocation
    : invocationName OPAREN CPAREN { $$ = FunctionInvocation::create(ast, $1); LOC($$, @$); }
    | invocationName OPAREN DISTINCT CPAREN { scanner.syntaxError(@$, "DISTINCT requires an argument"); }
    | invocationName OPAREN exprChain CPAREN { $$ = FunctionInvocation::create(ast, $1); $$->setArguments($3); LOC($$, @$); }
    | invocationName OPAREN DISTINCT exprChain CPAREN {
        $$ = FunctionInvocation::create(ast, $1);
        $$->setArguments($4);
        $$->setDistinct(true);
        LOC($$, @$);
      }
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
        nodePattern->setLabels($3);
        nodePattern->setProperties($4);
        nodePattern->setSymbol($2);
        $6->addRootEntity(nodePattern);
        LOC($$, @$);
      }
    | OPAREN nodeLabels CPAREN pathExprElem {
        $$ = PathExpr::create(ast, $4);
        NodePattern* node = NodePattern::create(ast);
        node->setLabels($2);
        $4->addRootEntity(node);
        LOC($$, @$);
        LOC(node, @$);
      }
    | OPAREN nodeLabels properties CPAREN pathExprElem {
        $$ = PathExpr::create(ast, $5);
        NodePattern* node = NodePattern::create(ast);
        node->setLabels($2);
        node->setProperties($3);
        $5->addRootEntity(node);
        LOC($$, @$);
        LOC(node, @$);
      }

    // Those three exprs are tricky and cause conflicts with 'OPAREN expr CPAREN'

    //| OPAREN symbol nodeLabels CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized exprs"); }
    // Causes conflicts because 'symbol nodeLabels' is a valid expr (propertyOrLabelExpr)

    //| OPAREN symbol CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized exprs"); }
    // Causes conflicts because 'symbol' is a valid expr (atomExpr)

    //| OPAREN properties CPAREN patternElemChain { scanner.notImplemented(@$, "Parenthesized Exprs"); }
    // Causes conflicts because 'properties' is a valid expr (map literal)

    // Instead, they are handled by the rule below

    | OPAREN expr CPAREN pathExprElem {
        $$ = PathExpr::create(ast, $4);

        if (NodePattern* nodePattern = NodePattern::fromExpr(ast, $2)) {
            $4->addRootEntity(nodePattern);
        } else {
            error(@1, "Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");
        }

        LOC($$, @$);
      }
    ;

pathExprElem
    : patternElemChain {
        $$ = PatternElement::create(ast);
        $$->addRootEntity($1.second);
        $$->addRootEntity($1.first);
        LOC($$, @$);
      }
    | pathExprElem patternElemChain {
        $$ = $1;
        $$->addRootEntity($2.second);
        $$->addRootEntity($2.first);
      }
    ;

parenthesizedExpr
    : OPAREN expr CPAREN { $$ = $2; }
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
//    : OBRACK edgesChainPattern PIPE expr CBRACK
//    | OBRACK lhs edgesChainPattern PIPE expr CBRACK
//    | OBRACK edgesChainPattern whereClause PIPE expr CBRACK
//    | OBRACK lhs edgesChainPattern whereClause PIPE expr CBRACK
//    ;

//edgesChainPattern
//    : nodePattern patternElemChain
//    | edgesChainPattern patternElemChain
//    ;

listComprehension
    : OBRACK filterExpr CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    | OBRACK filterExpr PIPE expr CBRACK { scanner.notImplemented(@$, "List comprehensions"); }
    ;

filterExpr
    : symbol IN expr { scanner.notImplemented(@$, "IN"); }
    | symbol IN expr whereClause { scanner.notImplemented(@$, "IN"); }
    ;

countFunc
    : COUNT OPAREN MULT CPAREN {
        Symbol* symbol = Symbol::create(ast, "count");
        QualifiedName* name = QualifiedName::create(ast);
        name->addName(symbol);
        $$ = FunctionInvocation::create(ast, name);
        ExprChain* arguments = ExprChain::create(ast);
        arguments->add(LiteralExpr::create(ast, WildcardLiteral::create(ast)));
        $$->setArguments(arguments);
        LOC($$, @$);
      }
    | COUNT OPAREN CPAREN { scanner.notImplemented(@$, "count()"); }
    | COUNT OPAREN DISTINCT CPAREN { scanner.notImplemented(@$, "count(DISTINCT)"); }
    | COUNT OPAREN exprChain CPAREN {
        Symbol* symbol = Symbol::create(ast, "count");
        QualifiedName* name = QualifiedName::create(ast);
        name->addName(symbol);
        $$ = FunctionInvocation::create(ast, name);
        $$->setArguments($3);
        LOC($$, @$);
      }
    | COUNT OPAREN DISTINCT exprChain CPAREN {
        Symbol* symbol = Symbol::create(ast, "count");
        QualifiedName* name = QualifiedName::create(ast);
        name->addName(symbol);
        $$ = FunctionInvocation::create(ast, name);
        $$->setArguments($4);
        $$->setDistinct(true);
        LOC($$, @$);
      }
    | COUNT OBRACE patternWhere CBRACE { scanner.notImplemented(@$, "count(pattern WHERE)"); }

    // Here, returnSt is mandatory for MATCH subqueries, as opposed to Neo4j's Cypher parser
    | COUNT OBRACE query CBRACE { scanner.notImplemented(@$, "count {...}"); }
    ;

caseExpr
    : CASE whenThenChain END { scanner.notImplemented(@$, "CASE"); }
    | CASE expr whenThenChain END  { scanner.notImplemented(@$, "CASE"); }
    | CASE whenThenChain ELSE expr END { scanner.notImplemented(@$, "CASE"); }
    | CASE expr whenThenChain ELSE expr END { scanner.notImplemented(@$, "CASE"); }
    ;

whenThenChain
    : whenThen
    | whenThenChain whenThen
    ;

whenThen
    : WHEN expr THEN expr
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
    | embeddingLit { $$ = $1; }
    | listLit { $$ = $1; }
    | mapLit { $$ = $1; }
    ;

boolLit
    : TRUE { $$ = BoolLiteral::create(ast, true); }
    | FALSE { $$ = BoolLiteral::create(ast, false); }
    ;

numLit
    : DIGIT { $$ = IntegerLiteral::create(ast, $1); }
    | DOUBLE { $$ = DoubleLiteral::create(ast, $1); }
    ;

stringLit
    : STRING_LITERAL { $$ = StringLiteral::create(ast, $1); }
    ;

embeddingLit
    : OPAREN embeddingLitItems CPAREN { $$ = $2; LOC($$, @$); }
    ;

embeddingLitItems
    : embeddingLitItem COMMA embeddingLitItem { $$ = EmbeddingLiteral::create(ast); $$->addItem($1); $$->addItem($3); LOC($$, @$); }
    | embeddingLitItems COMMA embeddingLitItem { $$ = $1; $$->addItem($3); }
    ;

embeddingLitItem
    : DIGIT { $$ = $1; LOC($$, @$); }
    | DOUBLE { $$ = $1; LOC($$, @$); }
    ;

// List literal: build directly
listLit
    : OBRACK CBRACK { $$ = ListLiteral::create(ast); LOC($$, @$); }
    | OBRACK listLitItems CBRACK { $$ = $2; LOC($$, @$); }
    ;

// Build recursively using addItem()
listLitItems
    : listLitItem { $$ = ListLiteral::create(ast); $$->addItem($1); }
    | listLitItems COMMA listLitItem { $$ = $1; $$->addItem($3); }
    ;

listLitItem
    : propertyExpr { $$ = $1; LOC($$, @$); }
    | atomExpr { $$ = $1; LOC($$, @$); }
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
    : name COLON expr { $$ = std::make_pair($1, $3); }
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
    | EMBEDDING { $$ = Symbol::create(ast, $1); }
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
    : OPAREN entityTypeExpr CPAREN
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
    : PROCEDURES { $$ = Symbol::create(ast, $1); }
    | DESCENDING { $$ = Symbol::create(ast, $1); }
    | CONSTRAINT { $$ = Symbol::create(ast, $1); }
    | MANDATORY { $$ = Symbol::create(ast, $1); }
    | ASCENDING { $$ = Symbol::create(ast, $1); }
    | OPTIONAL { $$ = Symbol::create(ast, $1); }
    | CONTAINS { $$ = Symbol::create(ast, $1); }
    | DISTINCT { $$ = Symbol::create(ast, $1); }
    // | EXTRACT { $$ = Symbol::create(ast, $1); }
    | REQUIRE { $$ = Symbol::create(ast, $1); }
    | COLLECT { $$ = Symbol::create(ast, $1); }
    | SUBMIT { $$ = Symbol::create(ast, $1); }
    | CHANGE { $$ = Symbol::create(ast, $1); }
    | STARTS { $$ = Symbol::create(ast, $1); }
    | UNIQUE { $$ = Symbol::create(ast, $1); }
    // | FILTER { $$ = Symbol::create(ast, $1); }
    | SINGLE { $$ = Symbol::create(ast, $1); }
    | SCALAR { $$ = Symbol::create(ast, $1); }
    | UNWIND { $$ = Symbol::create(ast, $1); }
    | REMOVE { $$ = Symbol::create(ast, $1); }
    | RETURN { $$ = Symbol::create(ast, $1); }
    | CREATE { $$ = Symbol::create(ast, $1); }
    | DELETE { $$ = Symbol::create(ast, $1); }
    | DETACH { $$ = Symbol::create(ast, $1); }
    | EXISTS { $$ = Symbol::create(ast, $1); }
    | COMMIT { $$ = Symbol::create(ast, $1); }
    | LIMIT { $$ = Symbol::create(ast, $1); }
    | YIELD { $$ = Symbol::create(ast, $1); }
    | MATCH { $$ = Symbol::create(ast, $1); }
    | MERGE { $$ = Symbol::create(ast, $1); }
    | ORDER { $$ = Symbol::create(ast, $1); }
    | WHERE { $$ = Symbol::create(ast, $1); }
    | UNION { $$ = Symbol::create(ast, $1); }
    | FALSE { $$ = Symbol::create(ast, $1); }
    | COUNT { $$ = Symbol::create(ast, $1); }
    | HEADERS { $$ = Symbol::create(ast, $1); }
    | JSONL { $$ = Symbol::create(ast, $1); }
    | CSV { $$ = Symbol::create(ast, $1); }
    | FAIL { $$ = Symbol::create(ast, $1); }
    | ERROR_ { $$ = Symbol::create(ast, $1); }
    | LIST { $$ = Symbol::create(ast, $1); }
    | GRAPHS { $$ = Symbol::create(ast, $1); }
    | AVAILABLE { $$ = Symbol::create(ast, $1); }
    | DESC { $$ = Symbol::create(ast, $1); }
    | CALL { $$ = Symbol::create(ast, $1); }
    | NULL_ { $$ = Symbol::create(ast, $1); }
    | TRUE { $$ = Symbol::create(ast, $1); }
    | WHEN { $$ = Symbol::create(ast, $1); }
    | NONE { $$ = Symbol::create(ast, $1); }
    | THEN { $$ = Symbol::create(ast, $1); }
    | ELSE { $$ = Symbol::create(ast, $1); }
    | CASE { $$ = Symbol::create(ast, $1); }
    | ENDS { $$ = Symbol::create(ast, $1); }
    | DROP { $$ = Symbol::create(ast, $1); }
    | SHOW { $$ = Symbol::create(ast, $1); }
    | INSTALL { $$ = Symbol::create(ast, $1); }
    | EXPLAIN { $$ = Symbol::create(ast, $1); }
    | EXTENSIONS { $$ = Symbol::create(ast, $1); }
    | SKIP { $$ = Symbol::create(ast, $1); }
    | WITH { $$ = Symbol::create(ast, $1); }
    | NEW { $$ = Symbol::create(ast, $1); }
    | ANY { $$ = Symbol::create(ast, $1); }
    | SET { $$ = Symbol::create(ast, $1); }
    | ALL { $$ = Symbol::create(ast, $1); }
    | ASC { $$ = Symbol::create(ast, $1); }
    | NOT { $$ = Symbol::create(ast, $1); }
    | END { $$ = Symbol::create(ast, $1); }
    | XOR { $$ = Symbol::create(ast, $1); }
    | FOR { $$ = Symbol::create(ast, $1); }
    | ADD { $$ = Symbol::create(ast, $1); }
    | AND { $$ = Symbol::create(ast, $1); }
    | OR { $$ = Symbol::create(ast, $1); }
    | IN { $$ = Symbol::create(ast, $1); }
    | IS { $$ = Symbol::create(ast, $1); }
    | BY { $$ = Symbol::create(ast, $1); }
    | DO { $$ = Symbol::create(ast, $1); }
    | OF { $$ = Symbol::create(ast, $1); }
    | ON { $$ = Symbol::create(ast, $1); }
    | IF { $$ = Symbol::create(ast, $1); }
    | AS { $$ = Symbol::create(ast, $1); }
    ;

%%

void db::YCypherParser::error(const location_type& l, const std::string& m) {
    scanner.syntaxError(l, m);
}


