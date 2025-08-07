#include "QueryInterpreter.h"

#include "ChangeManager.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"
#include "views/GraphView.h"
#include "ASTContext.h"
#include "QueryParser.h"
#include "QueryAnalyzer.h"
#include "QueryPlanner.h"
#include "ExecutionContext.h"
#include "Executor.h"
#include "AnalyzeException.h"
#include "PlannerException.h"
#include "ParserException.h"
#include "PipelineException.h"

#include "Profiler.h"
#include "TuringTime.h"

using namespace db;

QueryInterpreter::QueryInterpreter(SystemManager* sysMan, JobSystem* jobSystem)
    : _sysMan(sysMan),
    _jobSystem(jobSystem)
{
}

QueryInterpreter::~QueryInterpreter() {
}

QueryStatus QueryInterpreter::execute(std::string_view query,
                                      std::string_view graphName,
                                      LocalMemory* mem,
                                      QueryCallback callback,
                                      QueryHeaderCallback headerCallback,
                                      CommitHash commitHash,
                                      ChangeID changeID) {
    Profile profile {"QueryInterpreter::execute"};

    const auto start = Clock::now();

    auto txRes = _sysMan->openTransaction(graphName, commitHash, changeID);
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
    ASTContext astCtxt;
    QueryParser parser(&astCtxt);
    QueryCommand* cmd {nullptr};
    try {
        cmd = parser.parse(query);
        if (!cmd) {
            return QueryStatus(QueryStatus::Status::PARSE_ERROR);
        }
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
    QueryAnalyzer analyzer(view, &astCtxt);
    try {
        analyzer.analyze(cmd);
    } catch (const AnalyzeException& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                           "Unknown exception occurred");
    }

    // Query plan
    QueryPlanner planner(view, mem, callback);
    try {
        if (!planner.plan(cmd)) {
            return QueryStatus(QueryStatus::Status::PLAN_ERROR);
        }
    } catch (const PlannerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                           "Unknown exception occurred");
    }

    headerCallback(cmd);

    // Execute
    ExecutionContext execCtxt(_sysMan, _jobSystem, view, graphName, commitHash, changeID, &txRes.value());
    try {
        Executor executor;
        executor->run(&execCtxt, planner.getPipeline());
    } catch (const PipelineException& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR,
                           std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR,
                           "Unknown exception occurred");
    }

    const auto end = Clock::now();

    auto res = QueryStatus(QueryStatus::Status::OK);
    res.setTotalTime(end - start);
    return res;
}
