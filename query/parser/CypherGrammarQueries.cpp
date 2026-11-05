#include "CypherGrammar.h"

#include <string>

#include "ChangeQuery.h"
#include "CommitQuery.h"
#include "CreateEdgePropertyIndexQuery.h"
#include "CreateGraphQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateVectorIndexQuery.h"
#include "CypherAST.h"
#include "DeleteVectorIndexQuery.h"
#include "DropIndexQuery.h"
#include "EdgePattern.h"
#include "ExplainRequest.h"
#include "InstallExtensionQuery.h"
#include "ListAvailableGraphsQuery.h"
#include "ListGraphQuery.h"
#include "LoadCommitQuery.h"
#include "LoadEmbeddingQuery.h"
#include "LoadGMLQuery.h"
#include "LoadGraphQuery.h"
#include "LoadJsonlQuery.h"
#include "LoadParquetQuery.h"
#include "LoadVectorQuery.h"
#include "MergeDataPartsQuery.h"
#include "NodePattern.h"
#include "RegularQuery.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "ShowExtensionsQuery.h"
#include "ShowProceduresQuery.h"
#include "ShowVectorIndexesQuery.h"
#include "SinglePartQuery.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "expr/PropertyExpr.h"

#include "BioAssert.h"

using namespace db;

void CypherGrammar::parseScript() {
    _stackBase = reinterpret_cast<uintptr_t>(__builtin_frame_address(0));

    parseQueries();
    expect(CypherTokenKind::PROG_END);
}

Expr* CypherGrammar::parseExpression(size_t& endOffset) {
    _stackBase = reinterpret_cast<uintptr_t>(__builtin_frame_address(0));

    Expr* expr = parseExpr(ExprFollower::NONE);
    endOffset = _previous._location._endOffset;

    while (accept(CypherTokenKind::SEMI_COLON)) {
    }

    expect(CypherTokenKind::PROG_END);

    return expr;
}

void CypherGrammar::parseQueries() {
    parseQuery();

    while (accept(CypherTokenKind::SEMI_COLON)) {
        if (check(CypherTokenKind::PROG_END)) {
            return;
        }

        parseQuery();
    }
}

QueryCommand* CypherGrammar::parseQuery() {
    if (check(CypherTokenKind::EXPLAIN)) {
        return parseExplainQuery();
    }

    return parseRegularQuery();
}

QueryCommand* CypherGrammar::parseRegularQuery() {
    const SourcePosition start = currentPosition();

    const AdminQuery admin = findAdminQuery();
    if (admin != AdminQuery::NONE) {
        return parseSingleQuery(admin);
    }

    RegularQuery* segment = nullptr;
    if (check(CypherTokenKind::WHEN)) {
        segment = parseConditionalBranches();
    } else {
        const bool braced = check(CypherTokenKind::OBRACE);
        const UnionQuery::Branch operand = parseUnionOperand();
        const bool unites = braced || check(CypherTokenKind::UNION);

        if (!unites && !check(CypherTokenKind::NEXT)) {
            ParserUtils::markStandaloneCall(operand._query);
            return operand._query;
        } else if (!unites) {
            segment = operand._query;
        } else {
            UnionQuery::Branches branches;
            parseUnionList(branches);

            if (!check(CypherTokenKind::NEXT)) {
                ParserUtils::markStandaloneCall(operand._query);
                for (const UnionQuery::Branch& branch : branches) {
                    ParserUtils::markStandaloneCall(branch._query);
                }

                UnionQuery* query = UnionQuery::create(_ast, operand._query, branches);
                locate(query, start);

                return query;
            }

            segment = UnionQuery::create(_ast, operand._query, branches);
            locate(segment, start);
        }
    }

    if (!check(CypherTokenKind::NEXT)) {
        return segment;
    }

    return parseNextQuery(segment, start);
}

SinglePartQuery* CypherGrammar::parseNextQuery(RegularQuery* first, const SourcePosition& start) {
    ParserUtils::NextSegments segments;
    parseNextChain(first, locationFrom(start), segments);

    return ParserUtils::createNextQuery(_ast, segments, locationFrom(start));
}

void CypherGrammar::parseNextChain(RegularQuery* first, const SourceLocation& firstLocation, ParserUtils::NextSegments& segments) {
    RegularQuery* last = parseNextHead(first, firstLocation, segments);
    segments.push_back(last);
}

RegularQuery* CypherGrammar::parseNextHead(RegularQuery* first, const SourceLocation& firstLocation, ParserUtils::NextSegments& segments) {
    segments.push_back(parseSegmentBeforeNext(first, firstLocation));

    SourcePosition start = currentPosition();
    RegularQuery* segment = parseNextSegment();

    while (check(CypherTokenKind::NEXT)) {
        segments.push_back(parseSegmentBeforeNext(segment, locationFrom(start)));

        start = currentPosition();
        segment = parseNextSegment();
    }

    return segment;
}

RegularQuery* CypherGrammar::parseSegmentBeforeNext(RegularQuery* segment, const SourceLocation& location) {
    expect(CypherTokenKind::NEXT);

    if (ParserUtils::returnsUnaliasedItem(segment)) {
        reportSemanticError(location, "Expression in a RETURN followed by NEXT must be aliased (use AS)");
    }

    return segment;
}

RegularQuery* CypherGrammar::parseNextSegment() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::WHEN)) {
        return parseConditionalBranches();
    }

    const bool braced = check(CypherTokenKind::OBRACE);
    const UnionQuery::Branch operand = parseUnionOperand();
    if (!braced && !check(CypherTokenKind::UNION)) {
        return operand._query;
    }

    UnionQuery::Branches branches;
    parseUnionList(branches);

    UnionQuery* query = UnionQuery::create(_ast, operand._query, branches);
    locate(query, start);

    return query;
}

QueryCommand* CypherGrammar::parseExplainQuery() {
    expect(CypherTokenKind::EXPLAIN);

    if (accept(CypherTokenKind::OPAREN)) {
        parseExplainOptions();
        expect(CypherTokenKind::CPAREN);

        return parseRegularQuery();
    }

    QueryCommand* query = parseRegularQuery();

    ExplainRequest& explain = _ast->explainRequest();
    explain.requestDefaults();

    return query;
}

void CypherGrammar::parseExplainOptions() {
    parseExplainOption();

    while (accept(CypherTokenKind::COMMA)) {
        parseExplainOption();
    }
}

void CypherGrammar::parseExplainOption() {
    ExplainRequest& explain = _ast->explainRequest();

    if (accept(CypherTokenKind::ALL)) {
        explain.requestAll();
        return;
    }

    const SourceLocation wordLocation = currentLocation();
    const std::string_view word = parseExplainWord();

    if (accept(CypherTokenKind::ID)) {
        if (!explain.requestPass(word, _previous._text)) {
            reportUnknownExplainOption(wordLocation, word);
        }
    } else if (accept(CypherTokenKind::OBRACK)) {
        std::vector<std::string_view> passNames;
        parseExplainPassNames(passNames);
        expect(CypherTokenKind::CBRACK);

        for (const std::string_view passName : passNames) {
            if (!explain.requestPass(word, passName)) {
                reportUnknownExplainOption(wordLocation, word);
            }
        }
    } else if (!explain.requestStage(word)) {
        reportUnknownExplainOption(wordLocation, word);
    }
}

std::string_view CypherGrammar::parseExplainWord() {
    const SourceLocation location = currentLocation();
    expect(CypherTokenKind::ID);

    const std::string_view word = _previous._text;
    if (!ExplainRequest::isOptionWord(word)) {
        reportUnknownExplainOption(location, word);
    }

    return word;
}

void CypherGrammar::reportUnknownExplainOption(const SourceLocation& location, std::string_view word) {
    std::string message;
    ExplainRequest::describeOptions(word, message);

    reportSemanticError(location, message);
}

void CypherGrammar::parseExplainPassNames(std::vector<std::string_view>& names) {
    expect(CypherTokenKind::ID);
    names.push_back(_previous._text);

    while (accept(CypherTokenKind::COMMA)) {
        expect(CypherTokenKind::ID);
        names.push_back(_previous._text);
    }
}

void CypherGrammar::parseUnionList(UnionQuery::Branches& branches) {
    branches.push_back(parseUnionStmt());

    while (check(CypherTokenKind::UNION)) {
        branches.push_back(parseUnionStmt());
    }
}

UnionQuery::Branch CypherGrammar::parseUnionStmt() {
    expect(CypherTokenKind::UNION);
    const bool all = accept(CypherTokenKind::ALL);

    UnionQuery::Branch branch = parseUnionOperand();
    if (all) {
        branch._all = true;
    }

    return branch;
}

UnionQuery::Branch CypherGrammar::parseUnionOperand() {
    if (accept(CypherTokenKind::OBRACE)) {
        RegularQuery* body = parseQueryBody();
        expect(CypherTokenKind::CBRACE);

        return UnionQuery::Branch {._query = body};
    }

    return UnionQuery::Branch {._query = parseSinglePartQuery()};
}

QueryCommand* CypherGrammar::parseSingleQuery(AdminQuery admin) {
    const SourcePosition start = currentPosition();

    switch (admin) {
        case AdminQuery::CREATE_CONSTRAINT:
            parseCreateConstraint();
            reportNotImplemented(locationFrom(start), "CREATE CONSTRAINT");
            return nullptr;
        break;
        case AdminQuery::DROP_CONSTRAINT:
            parseDropConstraint();
            reportNotImplemented(locationFrom(start), "DROP CONSTRAINT");
            return nullptr;
        break;
        case AdminQuery::CREATE_GRAPH:
            return parseCreateGraphQuery();
        break;
        case AdminQuery::CREATE_VECTOR_INDEX:
            return parseCreateVectorIndexQuery();
        break;
        case AdminQuery::CREATE_NODE_PROPERTY_INDEX:
            return parseCreateNodePropertyIndexQuery();
        break;
        case AdminQuery::CREATE_EDGE_PROPERTY_INDEX:
            return parseCreateEdgePropertyIndexQuery();
        break;
        case AdminQuery::DROP_INDEX:
            return parseDropIndexQuery();
        break;
        case AdminQuery::LOAD_GRAPH:
            return parseLoadGraph();
        break;
        case AdminQuery::LOAD_JSONL:
            return parseLoadJsonl();
        break;
        case AdminQuery::LOAD_GML:
            return parseLoadGML();
        break;
        case AdminQuery::LOAD_PARQUET:
            return parseLoadParquet();
        break;
        case AdminQuery::LOAD_COMMIT:
            return parseLoadCommitQuery();
        break;
        case AdminQuery::LOAD_VECTOR:
            return parseLoadVectorQuery();
        break;
        case AdminQuery::LOAD_EMBEDDING:
            return parseLoadEmbedding();
        break;
        case AdminQuery::LIST_GRAPH:
            return parseListGraphQuery();
        break;
        case AdminQuery::LIST_AVAILABLE_GRAPHS:
            return parseListAvailableGraphsQuery();
        break;
        case AdminQuery::SHOW_PROCEDURES:
            return parseShowProceduresQuery();
        break;
        case AdminQuery::SHOW_EXTENSIONS:
            return parseShowExtensionsQuery();
        break;
        case AdminQuery::SHOW_VECTOR_INDEXES:
            return parseShowVectorIndexesQuery();
        break;
        case AdminQuery::S3_CONNECT:
            return parseS3ConnectQuery();
        break;
        case AdminQuery::S3_TRANSFER:
            return parseS3TransferQuery();
        break;
        case AdminQuery::DELETE_VECTOR_INDEX:
            return parseDeleteVectorIndexQuery();
        break;
        case AdminQuery::CHANGE:
            return parseChangeQuery();
        break;
        case AdminQuery::COMMIT:
            return parseCommitQuery();
        break;
        case AdminQuery::MERGE_DATAPARTS:
            return parseMergeDataPartsQuery();
        break;
        case AdminQuery::INSTALL_EXTENSION:
            return parseInstallExtensionQuery();
        break;
        case AdminQuery::NONE:
        break;
    }

    bioassert(false, "An administrative query was dispatched without a kind");
    return nullptr;
}

CypherGrammar::AdminQuery CypherGrammar::findAdminQuery() {
    const CypherTokenKind first = examineKind();
    const CypherTokenKind second = peek(1)._kind;

    if (first == CypherTokenKind::CREATE) {
        const CypherTokenKind third = peek(2)._kind;
        const bool namesAPath = third == CypherTokenKind::ASSIGN;
        const TokenKinds nameOrAssign = getNameKinds() | kindsOf({CypherTokenKind::ASSIGN});

        if (second == CypherTokenKind::CONSTRAINT && CypherToken::isSymbol(third)) {
            return AdminQuery::CREATE_CONSTRAINT;
        } else if (second == CypherTokenKind::CONSTRAINT && !namesAPath) {
            throwUnexpectedTokenAt(2, getSymbolKinds() | kindsOf({CypherTokenKind::ASSIGN}));
        } else if (second == CypherTokenKind::GRAPH && CypherToken::isName(third)) {
            return AdminQuery::CREATE_GRAPH;
        } else if (second == CypherTokenKind::GRAPH && !namesAPath) {
            throwUnexpectedTokenAt(2, nameOrAssign);
        } else if (second == CypherTokenKind::VECTOR && third == CypherTokenKind::INDEX) {
            return AdminQuery::CREATE_VECTOR_INDEX;
        } else if (second == CypherTokenKind::VECTOR && !namesAPath) {
            throwUnexpectedTokenAt(2, kindsOf({CypherTokenKind::ASSIGN, CypherTokenKind::INDEX}));
        } else if (second == CypherTokenKind::INDEX && CypherToken::isName(third)) {
            if (peek(3)._kind != CypherTokenKind::FOR) {
                throwUnexpectedTokenAt(3, kindsOf({CypherTokenKind::FOR}));
            }

            const CypherTokenKind pattern = peek(4)._kind;
            if (pattern == CypherTokenKind::OPAREN) {
                return AdminQuery::CREATE_NODE_PROPERTY_INDEX;
            } else if (pattern == CypherTokenKind::OBRACK) {
                return AdminQuery::CREATE_EDGE_PROPERTY_INDEX;
            }

            throwUnexpectedTokenAt(4, kindsOf({CypherTokenKind::OPAREN, CypherTokenKind::OBRACK}));
        } else if (second == CypherTokenKind::INDEX && !namesAPath) {
            throwUnexpectedTokenAt(2, nameOrAssign);
        }

        return AdminQuery::NONE;
    } else if (first == CypherTokenKind::DROP) {
        if (second == CypherTokenKind::INDEX) {
            return AdminQuery::DROP_INDEX;
        } else if (second == CypherTokenKind::CONSTRAINT) {
            return AdminQuery::DROP_CONSTRAINT;
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::CONSTRAINT, CypherTokenKind::INDEX}));
    } else if (first == CypherTokenKind::LOAD) {
        if (second == CypherTokenKind::GRAPH) {
            return AdminQuery::LOAD_GRAPH;
        } else if (second == CypherTokenKind::JSONL) {
            return AdminQuery::LOAD_JSONL;
        } else if (second == CypherTokenKind::GML) {
            return AdminQuery::LOAD_GML;
        } else if (second == CypherTokenKind::PARQUET) {
            return AdminQuery::LOAD_PARQUET;
        } else if (second == CypherTokenKind::COMMIT) {
            return AdminQuery::LOAD_COMMIT;
        } else if (second == CypherTokenKind::VECTOR) {
            return AdminQuery::LOAD_VECTOR;
        } else if (second == CypherTokenKind::EMBEDDING) {
            return AdminQuery::LOAD_EMBEDDING;
        } else if (second == CypherTokenKind::CSV) {
            return AdminQuery::NONE;
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::GRAPH,
                                           CypherTokenKind::JSONL,
                                           CypherTokenKind::GML,
                                           CypherTokenKind::PARQUET,
                                           CypherTokenKind::COMMIT,
                                           CypherTokenKind::VECTOR,
                                           CypherTokenKind::EMBEDDING,
                                           CypherTokenKind::CSV}));
    } else if (first == CypherTokenKind::LIST) {
        if (second == CypherTokenKind::GRAPH) {
            return AdminQuery::LIST_GRAPH;
        } else if (second == CypherTokenKind::AVAILABLE) {
            return AdminQuery::LIST_AVAILABLE_GRAPHS;
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::GRAPH, CypherTokenKind::AVAILABLE}));
    } else if (first == CypherTokenKind::SHOW) {
        if (second == CypherTokenKind::PROCEDURES) {
            return AdminQuery::SHOW_PROCEDURES;
        } else if (second == CypherTokenKind::EXTENSIONS) {
            return AdminQuery::SHOW_EXTENSIONS;
        } else if (second == CypherTokenKind::VECTOR) {
            return AdminQuery::SHOW_VECTOR_INDEXES;
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::PROCEDURES, CypherTokenKind::EXTENSIONS, CypherTokenKind::VECTOR}));
    } else if (first == CypherTokenKind::S3) {
        if (second == CypherTokenKind::CONNECT) {
            return AdminQuery::S3_CONNECT;
        } else if (second == CypherTokenKind::PULL || second == CypherTokenKind::PUSH) {
            return AdminQuery::S3_TRANSFER;
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::CONNECT, CypherTokenKind::PUSH, CypherTokenKind::PULL}));
    } else if (first == CypherTokenKind::DELETE) {
        const bool deletesAVectorIndex = second == CypherTokenKind::VECTOR && peek(2)._kind == CypherTokenKind::INDEX;
        return deletesAVectorIndex ? AdminQuery::DELETE_VECTOR_INDEX : AdminQuery::NONE;
    } else if (first == CypherTokenKind::CHANGE) {
        return AdminQuery::CHANGE;
    } else if (first == CypherTokenKind::COMMIT) {
        return AdminQuery::COMMIT;
    } else if (first == CypherTokenKind::MERGE_DATAPARTS) {
        return AdminQuery::MERGE_DATAPARTS;
    } else if (first == CypherTokenKind::INSTALL) {
        return AdminQuery::INSTALL_EXTENSION;
    }

    return AdminQuery::NONE;
}

LoadGraphQuery* CypherGrammar::parseLoadGraph() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::GRAPH);

    Symbol* name = parseName();

    LoadGraphQuery* query = LoadGraphQuery::create(_ast, name->getName());
    locate(query, start);

    return query;
}

LoadJsonlQuery* CypherGrammar::parseLoadJsonl() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::JSONL);
    expect(CypherTokenKind::STRING_LITERAL);

    const std::string_view path = _previous._text;

    Symbol* graphName = nullptr;
    if (accept(CypherTokenKind::AS)) {
        graphName = parseName();
    }

    JsonlImportSpecs specs;
    const bool hasOptions = check(CypherTokenKind::WITH);
    if (hasOptions) {
        parseJsonlOptions(specs);
    }

    LoadJsonlQuery* query = LoadJsonlQuery::create(_ast, fs::Path(std::string(path)));

    if (graphName) {
        query->setGraphName(graphName->getName());
    }

    if (hasOptions) {
        query->setEmbeddingSpecs(std::move(specs._embeddings));
        query->setDateTimeSpecs(std::move(specs._dateTimes));
        query->setDurationSpecs(std::move(specs._durations));
    }

    locate(query, start);

    return query;
}

void CypherGrammar::parseJsonlOptions(JsonlImportSpecs& specs) {
    parseJsonlOption(specs);

    while (check(CypherTokenKind::WITH)) {
        JsonlImportSpecs option;
        parseJsonlOption(option);
        specs.absorb(std::move(option));
    }
}

void CypherGrammar::parseJsonlOption(JsonlImportSpecs& specs) {
    expect(CypherTokenKind::WITH);

    if (accept(CypherTokenKind::EMBEDDINGS)) {
        expect(CypherTokenKind::OBRACK);
        parseEmbeddingSpecs(specs._embeddings);
        expect(CypherTokenKind::CBRACK);
    } else if (accept(CypherTokenKind::DATETIMES)) {
        expect(CypherTokenKind::OBRACK);
        parseSpecNames(specs._dateTimes);
        expect(CypherTokenKind::CBRACK);
    } else if (accept(CypherTokenKind::DURATIONS)) {
        expect(CypherTokenKind::OBRACK);
        parseSpecNames(specs._durations);
        expect(CypherTokenKind::CBRACK);
    } else {
        throwUnexpectedToken();
    }
}

template <typename Spec>
void CypherGrammar::parseSpecNames(Spec& specs) {
    do {
        if (accept(CypherTokenKind::STRING_LITERAL)) {
            specs.emplace(_previous._text);
        } else {
            specs.emplace(parseName()->getName());
        }
    } while (accept(CypherTokenKind::COMMA));
}

void CypherGrammar::parseEmbeddingSpecs(EmbeddingsSpec& specs) {
    do {
        const std::pair<std::string_view, size_t> spec = parseEmbeddingSpec();
        specs.emplace(spec.first, spec.second);
    } while (accept(CypherTokenKind::COMMA));
}

std::pair<std::string_view, size_t> CypherGrammar::parseEmbeddingSpec() {
    expect(CypherTokenKind::OBRACE);

    std::string_view name;
    if (accept(CypherTokenKind::STRING_LITERAL)) {
        name = _previous._text;
    } else {
        name = parseName()->getName();
    }

    expect(CypherTokenKind::COMMA);
    expect(CypherTokenKind::DIGIT);
    const size_t dimension = static_cast<size_t>(_previous._integer);
    expect(CypherTokenKind::CBRACE);

    return {name, dimension};
}

ListGraphQuery* CypherGrammar::parseListGraphQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LIST);
    expect(CypherTokenKind::GRAPH);

    ListGraphQuery* query = ListGraphQuery::create(_ast);
    locate(query, start);

    return query;
}

ListAvailableGraphsQuery* CypherGrammar::parseListAvailableGraphsQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LIST);
    expect(CypherTokenKind::AVAILABLE);
    expect(CypherTokenKind::GRAPHS);

    ListAvailableGraphsQuery* query = ListAvailableGraphsQuery::create(_ast);
    locate(query, start);

    return query;
}

ShowProceduresQuery* CypherGrammar::parseShowProceduresQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SHOW);
    expect(CypherTokenKind::PROCEDURES);

    ShowProceduresQuery* query = ShowProceduresQuery::create(_ast);
    locate(query, start);

    return query;
}

ShowExtensionsQuery* CypherGrammar::parseShowExtensionsQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SHOW);
    expect(CypherTokenKind::EXTENSIONS);

    ShowExtensionsQuery* query = ShowExtensionsQuery::create(_ast);
    locate(query, start);

    return query;
}

InstallExtensionQuery* CypherGrammar::parseInstallExtensionQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::INSTALL);

    Symbol* name = parseName();

    InstallExtensionQuery* query = InstallExtensionQuery::create(_ast, name->getName());
    locate(query, start);

    return query;
}

CreateVectorIndexQuery* CypherGrammar::parseCreateVectorIndexQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CREATE);
    expect(CypherTokenKind::VECTOR);
    expect(CypherTokenKind::INDEX);

    Symbol* name = parseName();

    expect(CypherTokenKind::WITH);
    expect(CypherTokenKind::DIMENSION);
    expect(CypherTokenKind::DIGIT);
    const int64_t dimension = _previous._integer;

    expect(CypherTokenKind::METRIC);
    const vec::DistanceMetric metric = parseDistanceMetric();

    vec::IndexType type = vec::IndexType::FLAT;
    if (accept(CypherTokenKind::TYPE)) {
        type = parseIndexType();
    }

    CreateVectorIndexQuery* query = CreateVectorIndexQuery::create(_ast, name->getName(), dimension, metric, type);
    locate(query, start);

    return query;
}

vec::IndexType CypherGrammar::parseIndexType() {
    if (accept(CypherTokenKind::HNSW)) {
        return vec::IndexType::HNSW;
    } else if (accept(CypherTokenKind::FLAT)) {
        return vec::IndexType::FLAT;
    }

    throwUnexpectedToken();
}

CreateNodePropertyIndexQuery* CypherGrammar::parseCreateNodePropertyIndexQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CREATE);
    expect(CypherTokenKind::INDEX);

    Symbol* name = parseName();
    expect(CypherTokenKind::FOR);

    const SourcePosition patternStart = currentPosition();
    NodePattern* pattern = parseNodePattern();
    const SourceLocation patternLocation = locationFrom(patternStart);

    expect(CypherTokenKind::ON);
    PropertyExpr* property = parseVariablePropertyExpr();

    if (pattern->getWhere()) {
        reportSemanticError(patternLocation, "WHERE is not allowed in an index pattern");
    }

    CreateNodePropertyIndexQuery* query = CreateNodePropertyIndexQuery::create(_ast, name->getName(), pattern, property);
    locate(query, start);

    return query;
}

CreateEdgePropertyIndexQuery* CypherGrammar::parseCreateEdgePropertyIndexQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CREATE);
    expect(CypherTokenKind::INDEX);

    Symbol* name = parseName();
    expect(CypherTokenKind::FOR);
    expect(CypherTokenKind::OBRACK);

    const bool emptyDetail = check(CypherTokenKind::CBRACK);
    const SourcePosition detailStart = currentPosition();
    EdgePattern* edge = parseEdgeDetail();
    const SourceLocation detailLocation = emptyDetail ? emptyLocation() : locationFrom(detailStart);

    expect(CypherTokenKind::CBRACK);
    expect(CypherTokenKind::ON);
    PropertyExpr* property = parseVariablePropertyExpr();

    if (edge->getWhere()) {
        reportSemanticError(detailLocation, "WHERE is not allowed in an index pattern");
    } else if (edge->getQuantifiedPath()) {
        reportSemanticError(detailLocation, "An edge property index takes no length quantifier");
    }

    CreateEdgePropertyIndexQuery* query = CreateEdgePropertyIndexQuery::create(_ast, name->getName(), edge, property);
    locate(query, start);

    return query;
}

DropIndexQuery* CypherGrammar::parseDropIndexQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::DROP);
    expect(CypherTokenKind::INDEX);

    Symbol* name = parseName();

    DropIndexQuery* query = DropIndexQuery::create(_ast, name->getName());
    locate(query, start);

    return query;
}

vec::DistanceMetric CypherGrammar::parseDistanceMetric() {
    if (accept(CypherTokenKind::EUCLID)) {
        return vec::DistanceMetric::EUCLIDEAN_DIST;
    } else if (accept(CypherTokenKind::COSINE)) {
        return vec::DistanceMetric::INNER_PRODUCT;
    }

    throwUnexpectedToken();
}

LoadVectorQuery* CypherGrammar::parseLoadVectorQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::VECTOR);
    expect(CypherTokenKind::FROM);
    expect(CypherTokenKind::STRING_LITERAL);

    const std::string_view path = _previous._text;

    expect(CypherTokenKind::IN);
    Symbol* index = parseName();

    LoadVectorQuery* query = LoadVectorQuery::create(_ast, path, index->getName());
    locate(query, start);

    return query;
}

LoadEmbeddingQuery* CypherGrammar::parseLoadEmbedding() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::EMBEDDING);
    expect(CypherTokenKind::FROM);
    expect(CypherTokenKind::STRING_LITERAL);

    const std::string_view path = _previous._text;

    expect(CypherTokenKind::AS);
    Symbol* property = parseName();

    LoadEmbeddingQuery* query = LoadEmbeddingQuery::create(_ast, path, property->getName());
    locate(query, start);

    return query;
}

DeleteVectorIndexQuery* CypherGrammar::parseDeleteVectorIndexQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::DELETE);
    expect(CypherTokenKind::VECTOR);
    expect(CypherTokenKind::INDEX);

    Symbol* name = parseName();

    DeleteVectorIndexQuery* query = DeleteVectorIndexQuery::create(_ast, name->getName());
    locate(query, start);

    return query;
}

ShowVectorIndexesQuery* CypherGrammar::parseShowVectorIndexesQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SHOW);
    expect(CypherTokenKind::VECTOR);
    expect(CypherTokenKind::INDEXES);

    ShowVectorIndexesQuery* query = ShowVectorIndexesQuery::create(_ast);
    locate(query, start);

    return query;
}

LoadCommitQuery* CypherGrammar::parseLoadCommitQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::COMMIT);
    expect(CypherTokenKind::STRING_LITERAL);

    LoadCommitQuery* query = LoadCommitQuery::create(_ast, _previous._text);
    locate(query, start);

    return query;
}

CreateGraphQuery* CypherGrammar::parseCreateGraphQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CREATE);
    expect(CypherTokenKind::GRAPH);

    Symbol* name = parseName();

    CreateGraphQuery* query = CreateGraphQuery::create(_ast, name->getName());
    locate(query, start);

    return query;
}

LoadGMLQuery* CypherGrammar::parseLoadGML() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::GML);
    expect(CypherTokenKind::STRING_LITERAL);

    const std::string_view path = _previous._text;

    Symbol* graphName = nullptr;
    if (accept(CypherTokenKind::AS)) {
        graphName = parseName();
    }

    LoadGMLQuery* query = LoadGMLQuery::create(_ast, fs::Path(std::string(path)));

    if (graphName) {
        query->setGraphName(graphName->getName());
    }

    locate(query, start);

    return query;
}

LoadParquetQuery* CypherGrammar::parseLoadParquet() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::PARQUET);
    expect(CypherTokenKind::STRING_LITERAL);

    const std::string_view path = _previous._text;

    Symbol* graphName = nullptr;
    if (accept(CypherTokenKind::AS)) {
        graphName = parseName();
    }

    DurationSpec durations;
    const bool hasDurations = accept(CypherTokenKind::WITH);
    if (hasDurations) {
        expect(CypherTokenKind::DURATIONS);
        expect(CypherTokenKind::OBRACK);
        parseSpecNames(durations);
        expect(CypherTokenKind::CBRACK);
    }

    LoadParquetQuery* query = LoadParquetQuery::create(_ast, fs::Path(std::string(path)));

    if (graphName) {
        query->setGraphName(graphName->getName());
    }

    if (hasDurations) {
        query->setDurationSpecs(std::move(durations));
    }

    locate(query, start);

    return query;
}

S3ConnectQuery* CypherGrammar::parseS3ConnectQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::S3);
    expect(CypherTokenKind::CONNECT);

    expect(CypherTokenKind::STRING_LITERAL);
    const std::string_view accessId = _previous._text;

    expect(CypherTokenKind::STRING_LITERAL);
    const std::string_view secretKey = _previous._text;

    expect(CypherTokenKind::STRING_LITERAL);
    const std::string_view region = _previous._text;

    S3ConnectQuery* query = S3ConnectQuery::create(_ast, accessId, secretKey, region);
    locate(query, start);

    return query;
}

S3TransferQuery* CypherGrammar::parseS3TransferQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::S3);

    const bool pushes = accept(CypherTokenKind::PUSH);
    if (!pushes) {
        expect(CypherTokenKind::PULL);
    }

    expect(CypherTokenKind::STRING_LITERAL);
    const std::string_view first = _previous._text;

    expect(CypherTokenKind::STRING_LITERAL);
    const std::string_view second = _previous._text;

    S3TransferQuery* query = pushes
                               ? S3TransferQuery::create(_ast, S3TransferQuery::Direction::PUSH, second, first)
                               : S3TransferQuery::create(_ast, S3TransferQuery::Direction::PULL, first, second);
    locate(query, start);

    return query;
}

ChangeQuery* CypherGrammar::parseChangeQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CHANGE);

    const ChangeOp operation = parseChangeOp();

    ChangeQuery* query = ChangeQuery::create(_ast, operation);
    locate(query, start);

    return query;
}

ChangeOp CypherGrammar::parseChangeOp() {
    if (accept(CypherTokenKind::NEW)) {
        return ChangeOp::NEW;
    } else if (accept(CypherTokenKind::SUBMIT)) {
        return ChangeOp::SUBMIT;
    } else if (accept(CypherTokenKind::DELETE)) {
        return ChangeOp::DELETE;
    } else if (accept(CypherTokenKind::LIST)) {
        return ChangeOp::LIST;
    }

    throwUnexpectedToken();
}

CommitQuery* CypherGrammar::parseCommitQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::COMMIT);

    CommitQuery* query = CommitQuery::create(_ast);
    locate(query, start);

    return query;
}

MergeDataPartsQuery* CypherGrammar::parseMergeDataPartsQuery() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::MERGE_DATAPARTS);

    MergeDataPartsQuery* query = MergeDataPartsQuery::create(_ast);
    locate(query, start);

    return query;
}

void CypherGrammar::parseCreateConstraint() {
    expect(CypherTokenKind::CREATE);
    expect(CypherTokenKind::CONSTRAINT);
    parseSymbol();

    if (accept(CypherTokenKind::IF)) {
        expect(CypherTokenKind::NOT);
        expect(CypherTokenKind::EXISTS);
    }

    parseForConstraint();
}

void CypherGrammar::parseForConstraint() {
    expect(CypherTokenKind::FOR);

    if (!check(CypherTokenKind::OPAREN)) {
        throwUnexpectedToken();
    }

    const CypherTokenKind next = peek(1)._kind;
    if (next == CypherTokenKind::CPAREN) {
        parseEdgeConstraintPattern();
    } else if (CypherToken::isSymbol(next)) {
        parseNodeConstraintPattern();
    } else {
        throwUnexpectedTokenAt(1, getSymbolKinds() | kindsOf({CypherTokenKind::CPAREN}));
    }

    expect(CypherTokenKind::REQUIRE);
    parseConstraint();
}

void CypherGrammar::parseNodeConstraintPattern() {
    expect(CypherTokenKind::OPAREN);

    const SourcePosition symbolStart = currentPosition();
    Symbol* symbol = parseSymbol();
    parseEntityTypeExpr(symbol, symbolStart);

    expect(CypherTokenKind::CPAREN);
}

void CypherGrammar::parseEdgeConstraintPattern() {
    expect(CypherTokenKind::OPAREN);
    expect(CypherTokenKind::CPAREN);
    expect(CypherTokenKind::SUB);
    expect(CypherTokenKind::OBRACK);

    parseSymbol();
    parseLabelExpression();

    expect(CypherTokenKind::CBRACK);
    expect(CypherTokenKind::SUB);
    expect(CypherTokenKind::OPAREN);
    expect(CypherTokenKind::CPAREN);
}

void CypherGrammar::parseConstraint() {
    Symbol* first = parseSymbol();

    if (!check(CypherTokenKind::DOT)) {
        throwUnexpectedToken();
    }

    parseQualifiedName(first);

    if (accept(CypherTokenKind::IS)) {
        expect(CypherTokenKind::UNIQUE);
        return;
    }

    expect(CypherTokenKind::IS_VALUE_TEST);
    expect(CypherTokenKind::NOT);
    expect(CypherTokenKind::NULL_);
}

void CypherGrammar::parseDropConstraint() {
    expect(CypherTokenKind::DROP);
    expect(CypherTokenKind::CONSTRAINT);
    parseSymbol();

    if (accept(CypherTokenKind::IF)) {
        expect(CypherTokenKind::EXISTS);
    }
}
