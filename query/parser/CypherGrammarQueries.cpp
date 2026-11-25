#include "CypherGrammar.h"

#include <string>

#include "CypherAST.h"
#include "ExplainRequest.h"
#include "RegularQuery.h"
#include "SinglePartQuery.h"

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
