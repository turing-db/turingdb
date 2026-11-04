#pragma once

#include <stddef.h>
#include <stdint.h>
#include <bitset>
#include <deque>
#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ChangeOp.h"
#include "ConditionalQuery.h"
#include "CypherToken.h"
#include "JsonlImportSpecs.h"
#include "ParserUtils.h"
#include "SourceLocation.h"
#include "UnionQuery.h"
#include "UnicodeNormalForm.h"
#include "VecLibMetadata.h"
#include "expr/CaseExpr.h"
#include "expr/MapProjectionExpr.h"
#include "expr/Operators.h"

namespace db {

class CallStmt;
class CallSubqueryStmt;
class ChangeQuery;
class CommitQuery;
class CreateEdgePropertyIndexQuery;
class CreateGraphQuery;
class CreateNodePropertyIndexQuery;
class CreateStmt;
class CreateVectorIndexQuery;
class CypherAST;
class CypherLexer;
class DeleteStmt;
class DeleteVectorIndexQuery;
class DropIndexQuery;
class EdgePattern;
class EmbeddingLiteral;
class EntityTypeExpr;
class Expr;
class ExprChain;
class FunctionInvocation;
class InstallExtensionQuery;
class LabelExpr;
class Limit;
class ListAvailableGraphsQuery;
class ListComprehensionExpr;
class ListGraphQuery;
class ListLiteral;
class Literal;
class BoolLiteral;
class LoadCommitQuery;
class LoadCSVStmt;
class LoadEmbeddingQuery;
class LoadGMLQuery;
class LoadGraphQuery;
class LoadJsonlQuery;
class LoadParquetQuery;
class LoadVectorQuery;
class MapLiteral;
class MatchStmt;
class MergeDataPartsQuery;
class MergeStmt;
class NodePattern;
class OrderBy;
class OrderByItem;
class Parameter;
class Pattern;
class PatternElement;
class Projection;
class PropertyExpr;
class QualifiedName;
class QuantifiedPath;
class QueryCommand;
class RegularQuery;
class RemoveStmt;
class ReturnStmt;
class S3ConnectQuery;
class S3TransferQuery;
class SetItem;
class SetStmt;
class ShortestPathStmt;
class ShowExtensionsQuery;
class ShowProceduresQuery;
class ShowVectorIndexesQuery;
class SinglePartQuery;
class Skip;
class SourceManager;
class Stmt;
class StmtContainer;
class StringLiteral;
class Symbol;
class SymbolExpr;
class UnwindStmt;
class VectorSearchStmt;
class WhereClause;
class WithStmt;
class YieldClause;
class YieldItems;

class CypherGrammar {
public:
    CypherGrammar(CypherAST* ast, CypherLexer* lexer);
    ~CypherGrammar();

    CypherGrammar(const CypherGrammar&) = delete;
    CypherGrammar(CypherGrammar&&) = delete;
    CypherGrammar& operator=(const CypherGrammar&) = delete;
    CypherGrammar& operator=(CypherGrammar&&) = delete;

    void parseScript();

private:
    using TokenKinds = std::bitset<CypherToken::KIND_COUNT>;

    enum class ExprFollower : uint8_t {
        NONE,
        AS,
        PIPE,
        PIPE_OR_COMMA,
    };

    enum class AdminQuery : uint8_t {
        NONE,
        CREATE_CONSTRAINT,
        CREATE_GRAPH,
        CREATE_VECTOR_INDEX,
        CREATE_NODE_PROPERTY_INDEX,
        CREATE_EDGE_PROPERTY_INDEX,
        DROP_INDEX,
        DROP_CONSTRAINT,
        LOAD_GRAPH,
        LOAD_JSONL,
        LOAD_GML,
        LOAD_PARQUET,
        LOAD_COMMIT,
        LOAD_VECTOR,
        LOAD_EMBEDDING,
        LIST_GRAPH,
        LIST_AVAILABLE_GRAPHS,
        SHOW_PROCEDURES,
        SHOW_EXTENSIONS,
        SHOW_VECTOR_INDEXES,
        S3_CONNECT,
        S3_TRANSFER,
        DELETE_VECTOR_INDEX,
        CHANGE,
        COMMIT,
        MERGE_DATAPARTS,
        INSTALL_EXTENSION,
    };

    enum class ParenthesizedAtom : uint8_t {
        EXPRESSION,
        PREDICATE_ROOT,
        EMBEDDING,
    };

    enum class OperatorLevel : uint8_t {
        NONE,
        OR,
        XOR,
        AND,
        NOT,
        COMPARISON,
        PREDICATE,
        ADD_SUB,
        MULT_DIV,
        POWER,
        SIGN,
    };

    struct SourcePosition {
        uint32_t _line {1};
        uint32_t _column {1};
        size_t _offset {0};
    };

    struct PendingOperator {
        CypherTokenKind _kind {CypherTokenKind::PROG_END};
        OperatorLevel _level {OperatorLevel::NONE};
        BinaryOperator _comparison {BinaryOperator::Equal};
        StringOperator _stringOperator {StringOperator::Contains};
        SourcePosition _start;
    };

    struct PendingOperand {
        Expr* _expr {nullptr};
        ParserUtils::ComparisonChain _chain;
        SourcePosition _start;
        bool _chained {false};
    };

    CypherAST* _ast {nullptr};
    SourceManager* _sourceManager {nullptr};
    CypherLexer* _lexer {nullptr};

    /* ***** Parser State ***** */

    // Tokens lexed ahead and not consumed yet; the front is the current token
    std::deque<CypherToken> _window;

    // The last consumed token, where the location of the node being built ends
    CypherToken _previous;

    // Number of tokens consumed, the index in the query of the front of _window; emptyLocation()
    // reads _previous only once it is not 0
    size_t _consumedCount {0};

    // Index one past the closer of each bracket a lookahead has skipped, 0 if it has none
    std::unordered_map<size_t, size_t> _closerEnds;

    // True if a check read the current token; reportSemanticError() then holds the error
    bool _examined {false};

    // Token kinds checked at the current token, for the "expecting" list of a syntax error
    TokenKinds _expected;

    // Token kinds checked at the next token; consume() moves them into _expected
    TokenKinds _pending;

    // Number of open brackets and CASE, at most MAX_NESTING_DEPTH
    size_t _depth {0};

    // Frame address of parseScript(), from which enterNesting() measures the stack in use
    uintptr_t _stackBase {0};

    // What may follow the current expression: AS before an alias, a PIPE that ends it, or in the
    // first item of a bracket, the PIPE of a comprehension or the COMMA of a list
    ExprFollower _follower {ExprFollower::NONE};

    // Nesting depth where _follower was set; _follower applies only at that depth
    size_t _followerDepth {0};

    // True if a semantic error waits for the next consume() to throw it
    bool _holdsError {false};

    // Location of the held semantic error
    SourceLocation _heldErrorLocation;

    // Message of the held semantic error
    std::string _heldErrorMessage;

    // Operators that parseExpr has read and not reduced yet, shared by all nesting levels
    std::vector<PendingOperator> _operators;

    // Operands that parseExpr has read and not reduced yet, shared by all nesting levels
    std::vector<PendingOperand> _operands;

    const CypherToken& peek(size_t offset);
    const CypherToken& examine();
    CypherTokenKind examineKind();
    bool check(CypherTokenKind kind);
    bool accept(CypherTokenKind kind);
    void expect(CypherTokenKind kind);
    bool checkName();
    bool checkSymbol();
    void consume();
    void addPending(CypherTokenKind kind);

    SourceLocation currentLocation();
    SourcePosition currentPosition();
    SourceLocation locationFrom(const SourcePosition& start) const;
    SourceLocation emptyLocation() const;
    void locate(const void* object, const SourcePosition& start);

    [[noreturn]] void throwUnexpectedToken();
    [[noreturn]] void throwUnexpectedTokenAt(size_t offset, const TokenKinds& kinds);
    void reportSemanticError(const SourceLocation& location, std::string_view message);
    void reportNotImplemented(const SourceLocation& location, std::string_view what);
    [[noreturn]] void throwUnexpected(const CypherToken& token, const TokenKinds& kinds);

    void enterNesting(const SourceLocation& location);
    void leaveNesting();
    bool isFollowedBy(ExprFollower follower) const;

    static const TokenKinds& getNameKinds();
    static const TokenKinds& getSymbolKinds();
    static const TokenKinds& getCallOpeningKinds();
    static TokenKinds kindsOf(std::initializer_list<CypherTokenKind> kinds);

    void parseQueries();
    QueryCommand* parseQuery();
    QueryCommand* parseRegularQuery();
    [[gnu::noinline]] SinglePartQuery* parseNextQuery(RegularQuery* first, const SourcePosition& start);
    void parseNextChain(RegularQuery* first, const SourceLocation& firstLocation, ParserUtils::NextSegments& segments);
    RegularQuery* parseNextHead(RegularQuery* first, const SourceLocation& firstLocation, ParserUtils::NextSegments& segments);
    RegularQuery* parseSegmentBeforeNext(RegularQuery* segment, const SourceLocation& location);
    RegularQuery* parseNextSegment();
    QueryCommand* parseExplainQuery();
    void parseExplainOptions();
    void parseExplainOption();
    std::string_view parseExplainWord();
    void reportUnknownExplainOption(const SourceLocation& location, std::string_view word);
    void parseExplainPassNames(std::vector<std::string_view>& names);
    void parseUnionList(UnionQuery::Branches& branches);
    UnionQuery::Branch parseUnionStmt();
    UnionQuery::Branch parseUnionOperand();
    QueryCommand* parseSingleQuery(AdminQuery admin);
    AdminQuery findAdminQuery();

    LoadGraphQuery* parseLoadGraph();
    LoadJsonlQuery* parseLoadJsonl();
    void parseJsonlOptions(JsonlImportSpecs& specs);
    void parseJsonlOption(JsonlImportSpecs& specs);
    template <typename Spec>
    void parseSpecNames(Spec& specs);
    void parseEmbeddingSpecs(EmbeddingsSpec& specs);
    std::pair<std::string_view, size_t> parseEmbeddingSpec();
    ListGraphQuery* parseListGraphQuery();
    ListAvailableGraphsQuery* parseListAvailableGraphsQuery();
    ShowProceduresQuery* parseShowProceduresQuery();
    ShowExtensionsQuery* parseShowExtensionsQuery();
    InstallExtensionQuery* parseInstallExtensionQuery();
    CreateVectorIndexQuery* parseCreateVectorIndexQuery();
    vec::IndexType parseIndexType();
    CreateNodePropertyIndexQuery* parseCreateNodePropertyIndexQuery();
    CreateEdgePropertyIndexQuery* parseCreateEdgePropertyIndexQuery();
    DropIndexQuery* parseDropIndexQuery();
    vec::DistanceMetric parseDistanceMetric();
    LoadVectorQuery* parseLoadVectorQuery();
    LoadEmbeddingQuery* parseLoadEmbedding();
    DeleteVectorIndexQuery* parseDeleteVectorIndexQuery();
    ShowVectorIndexesQuery* parseShowVectorIndexesQuery();
    LoadCommitQuery* parseLoadCommitQuery();
    CreateGraphQuery* parseCreateGraphQuery();
    LoadGMLQuery* parseLoadGML();
    LoadParquetQuery* parseLoadParquet();
    S3ConnectQuery* parseS3ConnectQuery();
    S3TransferQuery* parseS3TransferQuery();
    ChangeQuery* parseChangeQuery();
    ChangeOp parseChangeOp();
    CommitQuery* parseCommitQuery();
    MergeDataPartsQuery* parseMergeDataPartsQuery();
    void parseCreateConstraint();
    void parseForConstraint();
    void parseNodeConstraintPattern();
    void parseEdgeConstraintPattern();
    void parseConstraint();
    void parseDropConstraint();

    SinglePartQuery* parseSinglePartQuery();
    StmtContainer* parseQueryStatements();
    Stmt* parseQueryStatement();
    Stmt* parseReadingStatement();
    Stmt* parseUpdatingStatement();
    ReturnStmt* parseReturnStmt();
    WithStmt* parseWithStmt();
    Projection* parseProjectionBody();
    Projection* parseProjectionClauses();
    Projection* parseProjectionItems();
    Expr* parseProjectionItem();
    OrderBy* parseOptionalOrderBySubStmt();
    OrderBy* parseOrderBySubStmt();
    OrderByItem* parseOrderByItem();
    Skip* parseOptionalSkipSubStmt();
    Skip* parseSkipSubStmt();
    Limit* parseOptionalLimitSubStmt();
    Limit* parseLimitSubStmt();
    WhereClause* parseOptionalWhereClause();
    WhereClause* parseWhereClause(ExprFollower follower);
    MatchStmt* parseMatchStmt();
    ShortestPathStmt* parseShortestPathStmt();
    UnwindStmt* parseUnwindStmt();
    LoadCSVStmt* parseLoadCSVStmt();
    VectorSearchStmt* parseVectorSearchStmt();
    CallStmt* parseCallStmt();
    CallSubqueryStmt* parseCallSubqueryStmt();
    void parseCallScope(std::vector<const Symbol*>& imports);
    ExprChain* parseParenExprChain();
    ExprChain* parseExprChain();
    YieldClause* parseYieldClause();
    YieldItems* parseYieldItems();
    YieldItems* parseYieldItemChain();
    SymbolExpr* parseYieldItem();
    RegularQuery* parseQueryBody();
    ConditionalQuery* parseConditionalBranches();
    void parseWhenBranches(ConditionalQuery::Branches& branches);
    RegularQuery* parseConditionalBranchQuery();
    CallSubqueryStmt* parseForeachStmt();
    StmtContainer* parseForeachUpdates();
    CreateStmt* parseCreateStmt();
    MergeStmt* parseMergeStmt();
    std::pair<SetStmt*, SetStmt*> parseMergeActionChain();
    std::pair<SetStmt*, SetStmt*> parseMergeAction();
    SetStmt* parseSetStmt();
    SetItem* parseSetItem();
    RemoveStmt* parseRemoveStmt();
    PropertyExpr* parseRemoveItem();
    DeleteStmt* parseDeleteStmt();
    bool startsQueryStatement();
    bool startsReadingStatement();
    bool startsUpdatingStatement();
    bool opensCallSubquery(size_t offset);
    bool readsBracedPattern();

    Pattern* parsePattern();
    Pattern* parsePatternWhere();
    PatternElement* parsePatternPart();
    PatternElement* parsePatternAlias();
    PatternElement* parsePatternElem();
    std::pair<EdgePattern*, NodePattern*> parsePatternElemChain();
    EdgePattern* parseEdgePattern();
    EdgePattern* parseExprEdgePattern();
    EdgePattern* parseEdgeDetail();
    QuantifiedPath* parseOptionalEdgeRange();
    QuantifiedPath* parseEdgeRange();
    NodePattern* parseNodePattern();
    NodePattern* parseNodeDetail();
    LabelExpr* parseOptionalLabelExpression();
    MapLiteral* parseOptionalProperties();
    MapLiteral* parseProperties();
    QuantifiedPath* parseOptionalQuantifiedPath();
    QuantifiedPath* parseQuantifiedPath();
    QuantifiedPath* parseQuantifiedPathRange();
    LabelExpr* parseLabelExpression();
    LabelExpr* parseLabelDisjunction();
    LabelExpr* parseLabelConjunction();
    LabelExpr* parseLabelNegation();
    LabelExpr* parseLabelAtom();
    std::pair<EdgePattern*, NodePattern*> parseExprPatternElemChain();
    PatternElement* parsePathExprElem();
    Expr* parsePathExpr();
    Expr* parsePathExpr(NodePattern* root, const SourcePosition& rootStart);
    NodePattern* parsePredicateRoot();
    bool startsPatternElemChain();
    bool startsExprPatternElemChain();
    bool startsQuantifiedPath();
    size_t skipExprEdgePattern(size_t offset);
    size_t skipQuantifiedPath(size_t offset);
    size_t skipEntityDetail(size_t opener);
    size_t skipEdgeRange(size_t offset);

    Expr* parseExpr(ExprFollower follower);
    void parseOperand(size_t operatorBase);
    bool startsNegation(size_t operatorBase);
    // noinline keeps these frames off the recursive path: MAX_NESTING_DEPTH levels must
    // fit in half of a 512 KB thread stack
    [[gnu::noinline]] bool pushPrefixes(size_t operatorBase);
    [[gnu::noinline]] void pushOperand(Expr* expr, const SourcePosition& start, size_t operatorBase);
    [[gnu::noinline]] OperatorLevel findBinaryOperator(OperatorLevel tightest);
    [[gnu::noinline]] void pushBinaryOperator(OperatorLevel level);
    [[gnu::noinline]] void reduceOperators(size_t operatorBase, OperatorLevel level);
    void reducePrefix(const PendingOperator& pending);
    void reduceBinary(const PendingOperator& pending);
    [[gnu::noinline]] void applyIsTest();
    [[gnu::noinline]] Expr* popOperand();
    BinaryOperator parseComparisonSign();
    ParserUtils::IsTest parseIsTest(SourceLocation& testLocation);
    UnicodeNormalForm parseNormalForm();
    StringOperator parseStringExprPrefix();
    Expr* parseAtomicExpr();
    Expr* parseLookupOperand(Expr* operand, const SourcePosition& operandStart);
    EntityTypeExpr* parseEntityTypeExpr(Symbol* symbol, const SourcePosition& symbolStart);
    PropertyExpr* parsePropertyExpr(QualifiedName* name, const SourcePosition& nameStart);
    PropertyExpr* parseVariablePropertyExpr();
    QualifiedName* parseQualifiedName(Symbol* first);
    QualifiedName* parseInvocationName(Symbol* first);
    Expr* parseAtomExpr();
    Expr* parsePatternPredicate();
    Expr* parseLiteralExpr();
    Expr* parseParameterExpr();
    Expr* parseKeywordInvocationExpr();
    Expr* parseParenthesizedExpr();
    FunctionInvocation* parseFunctionInvocation(QualifiedName* name, const SourcePosition& nameStart);
    FunctionInvocation* parseCountFunc();
    FunctionInvocation* parseNormalizeFunc();
    Expr* parseCollectExpr();
    Expr* parseSubqueryExist();
    [[gnu::noinline]] Expr* parseExistsPredicate(const SourcePosition& start);
    Expr* parseSubqueryCount();
    [[gnu::noinline]] SinglePartQuery* parsePatternBody();
    Expr* parseFilterWith();
    ListComprehensionExpr* parseFilterExpr();
    Expr* parseReduceExpr();
    Expr* parseAllReduceExpr();
    Expr* parseMapProjection(Symbol* symbol, const SourceLocation& symbolLocation);
    void parseMapProjectionBody(MapProjectionExpr::Items& items);
    void parseMapProjectionItems(MapProjectionExpr::Items& items);
    MapProjectionExpr::Item parseMapProjectionItem();
    Expr* parseBracketExpr();
    std::pair<WhereClause*, Expr*> parseComprehensionTail();
    ListLiteral* parseListLiteralItems(Expr* firstItem, const SourceLocation& firstItemLocation);
    Expr* parseListLiteralItem();
    CaseExpr* parseCaseExpr();
    CaseExpr* parseWhenThenChain();
    Expr* parseWhenThen(CaseExpr::Tests& tests);
    void parseCaseTests(CaseExpr::Tests& tests);
    CaseExpr::Test parseCaseTest();

    Literal* parseLiteral();
    BoolLiteral* parseBoolLiteral();
    Literal* parseNumberLiteral();
    StringLiteral* parseStringLiteral();
    MapLiteral* parseMapLiteral();
    MapLiteral* parseMapPairChain();
    std::pair<Symbol*, Expr*> parseMapPair();
    EmbeddingLiteral* parseEmbeddingLiteral();
    EmbeddingLiteral* parseEmbeddingLiteralItems();
    double parseEmbeddingLiteralItem();
    Parameter* parseParameter();
    Symbol* parseName();
    Symbol* parseSymbol();

    bool opensConstruct(size_t offset, bool aliasMayFollow);
    bool braceOpensMapProjection(size_t offset);
    bool notOpensNegation(size_t offset, bool aliasMayFollow);
    bool distinctOpensProjection();
    bool distinctOpensArgument(bool closeKeepsKeyword);
    bool whenOpensBranch();
    bool whereOpensPredicate(size_t offset);
    bool whereOpensDetailPredicate(size_t offset);
    bool isOpensPredicateLabels(size_t offset);
    bool isOpensDetailLabels(size_t offset);
    bool pipeDisjoinsLabels();
    ParenthesizedAtom classifyParenthesis();
    bool startsPredicate();
    bool startsComparison();
    size_t skipBalanced(size_t offset);
    void matchBrackets(size_t offset);
    size_t skipLabelExpression(size_t offset);
    size_t skipLabelConjunction(size_t offset);
    size_t skipLabelAtom(size_t offset);

    static bool startsExpr(CypherTokenKind kind);
    static bool startsLabelAtom(CypherTokenKind kind);
    static bool isInfixWord(CypherTokenKind kind);
    static bool canFollowLabels(CypherTokenKind kind);
    static bool startsOperandOfKeyword(CypherTokenKind next, bool aliasMayFollow);
    static Expr* getExpr(const PendingOperand& operand);
};

}
