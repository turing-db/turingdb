#include "QueryInterpreterV2.h"

#include "InterpreterContext.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "versioning/Transaction.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "PlanGraphGenerator.h"
#include "ParserException.h"
#include "AnalyzeException.h"
#include "PlannerException.h"

#include "Profiler.h"

using namespace db;

QueryInterpreterV2::QueryInterpreterV2(SystemManager* sysMan,
                                       JobSystem* jobSystem)
    : _sysMan(sysMan),
    _jobSystem(jobSystem)
{
}

QueryInterpreterV2::~QueryInterpreterV2() {
}

QueryStatus QueryInterpreterV2::execute(const InterpreterContext& ctxt,
                                        std::string_view query,
                                        std::string_view graphName) {
    Profile profile {"QueryInterpreterV2::execute"};

    const auto start = Clock::now();

    auto txRes = _sysMan->openTransaction(graphName, ctxt.getCommitHash(), ctxt.getChangeID());
    if (!txRes) {
        switch (txRes.error().getType()) {
            case ChangeErrorType::GRAPH_NOT_FOUND:
                return QueryStatus(QueryStatus::Status::GRAPH_NOT_FOUND);
            case ChangeErrorType::CHANGE_NOT_FOUND:
                return QueryStatus(QueryStatus::Status::CHANGE_NOT_FOUND);
            default:
                return QueryStatus(QueryStatus::Status::COMMIT_NOT_FOUND);
        }
    }

    auto view = txRes->viewGraph();

    // Parsing query
    CypherAST ast(query);
    CypherParser parser(ast);
    try {
        parser.parse(query);
    } catch (const ParserException& e) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR,
                           "Unknown exception occurred");
    }

    // Analyze query
    CypherAnalyzer analyzer(ast, view);
    try {
        analyzer.analyze();
    } catch (const AnalyzeException& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                           "Unknown exception occurred");
    }

    // Generate plan graph
    PlanGraphGenerator planGen(view, ctxt.getQueryCallback());
    try {
        
    } catch (const PlannerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                           "Unknown exception occurred");
    }

    const auto end = Clock::now();

    auto res = QueryStatus(QueryStatus::Status::OK);
    res.setTotalTime(end - start);
    return res;
}
