#include "QueryInterpreter.h"

#include "GraphMetadata.h"
#include "SystemManager.h"
#include "Graph.h"
#include "views/GraphView.h"
#include "ASTContext.h"
#include "QueryParser.h"
#include "QueryAnalyzer.h"
#include "QueryPlanner.h"
#include "ExecutionContext.h"
#include "Executor.h"
#include "PlannerException.h"
#include "PipelineException.h"

#include "Time.h"

using namespace db;

QueryInterpreter::QueryInterpreter(SystemManager* sysMan)
    : _sysMan(sysMan),
    _executor(std::make_unique<Executor>())
{
}

QueryInterpreter::~QueryInterpreter() {
}

QueryStatus QueryInterpreter::execute(std::string_view query,
                                      std::string_view graphName,
                                      LocalMemory* mem,
                                      QueryCallback callback) {
    const auto start = Clock::now();

    Graph* graph = graphName.empty() ? _sysMan->getDefaultGraph() 
            : _sysMan->getGraph(std::string(graphName));
    if (!graph) {
        return QueryStatus(QueryStatus::Status::GRAPH_NOT_FOUND);
    }

    // Get view of the graph for the query
    [[maybe_unused]] const GraphView view = graph->view();

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
    ExecutionContext execCtxt(_sysMan, view);
    try {
        _executor->run(&execCtxt, planner.getPipeline());
    } catch (const PipelineException& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR, e.what());
    }

    const auto end = Clock::now();
    
    auto res = QueryStatus(QueryStatus::Status::OK);
    res.setTotalTime(end-start);
    return res;
}
