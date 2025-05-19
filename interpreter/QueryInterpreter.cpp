#include "QueryInterpreter.h"

#include <variant>

#include "ChangeManager.h"
#include "SystemManager.h"
#include "Graph.h"
#include "versioning/Transaction.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Commit.h"
#include "views/GraphView.h"
#include "ASTContext.h"
#include "QueryParser.h"
#include "QueryAnalyzer.h"
#include "QueryPlanner.h"
#include "ExecutionContext.h"
#include "Executor.h"
#include "PlannerException.h"
#include "PipelineException.h"

#include "Profiler.h"
#include "Time.h"

using namespace db;

QueryInterpreter::QueryInterpreter(SystemManager* sysMan, JobSystem* jobSystem)
    : _sysMan(sysMan),
      _jobSystem(jobSystem),
      _executor(std::make_unique<Executor>()) {
}

QueryInterpreter::~QueryInterpreter() {
}

QueryStatus QueryInterpreter::execute(std::string_view query,
                                      std::string_view graphName,
                                      LocalMemory* mem,
                                      QueryCallback callback,
                                      CommitHash commitHash,
                                      ChangeID changeID) {
    Profile profile {"QueryInterpreter::execute"};

    const auto start = Clock::now();

    WriteTransaction* writeTx = nullptr;
    auto txRes = openTransaction(graphName, commitHash, changeID);
    if (!txRes) {
        return txRes.error();
    }

    if (std::holds_alternative<WriteTransaction>(txRes.value())) {
        writeTx = &std::get<WriteTransaction>(txRes.value());
    }

    auto view = txRes->index() == 0
                  ? std::get<Transaction>(txRes.value()).viewGraph()
                  : std::get<WriteTransaction>(txRes.value()).viewGraph();

    // Parsing query
    ASTContext astCtxt;
    QueryParser parser(&astCtxt);
    QueryCommand* cmd = parser.parse(query);
    if (!cmd) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR);
    }

    // Analyze query
    QueryAnalyzer analyzer(&astCtxt, view.metadata().propTypes());
    if (!analyzer.analyze(cmd)) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR);
    }

    // Query plan
    QueryPlanner planner(view, mem, callback);
    try {
        if (!planner.plan(cmd)) {
            return QueryStatus(QueryStatus::Status::PLAN_ERROR);
        }
    } catch (const PlannerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    }

    // Execute
    ExecutionContext execCtxt(_sysMan, _jobSystem, view, graphName, commitHash, changeID, writeTx);
    try {
        _executor->run(&execCtxt, planner.getPipeline());
    } catch (const PipelineException& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR, e.what());
    }

    const auto end = Clock::now();

    auto res = QueryStatus(QueryStatus::Status::OK);
    res.setTotalTime(end - start);
    return res;
}

BasicResult<std::variant<Transaction, WriteTransaction>, QueryStatus> QueryInterpreter::openTransaction(std::string_view graphName,
                                                                                                        CommitHash commitHash,
                                                                                                        ChangeID changeID) {
    Graph* graph = graphName.empty() ? _sysMan->getDefaultGraph()
                                     : _sysMan->getGraph(std::string(graphName));
    if (!graph) {
        return BadResult<QueryStatus>(QueryStatus::Status::GRAPH_NOT_FOUND);
    }

    auto changeRes = _sysMan->getChangeManager().getChange(changeID);
    if (!changeRes) {
        if (auto tx = graph->openTransaction(commitHash); tx.isValid()) {
            return tx; // Ready-only transaction
        }

        return BadResult<QueryStatus>(QueryStatus::Status::COMMIT_NOT_FOUND);
    }

    auto* change = changeRes.value();

    if (auto tx = change->openWriteTransaction(); tx.isValid()) {
        return tx; // Write transaction
    }

    return BadResult<QueryStatus>(QueryStatus::Status::COMMIT_NOT_FOUND);
}
