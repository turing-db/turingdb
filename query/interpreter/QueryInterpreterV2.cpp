#include "QueryInterpreterV2.h"

#include "InterpreterContext.h"
#include "SystemManager.h"
#include "JobSystem.h"
#include "versioning/Transaction.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "PlanGraphGenerator.h"
#include "PipelineV2.h"
#include "PlanOptimizer.h"
#include "PipelineGenerator.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"

#include "PipelineException.h"
#include "CompilerException.h"

#include "Profiler.h"
#include "versioning/VersionControlException.h"

using namespace db;

QueryInterpreterV2::QueryInterpreterV2(db::SystemManager* sysMan,
                                       db::JobSystem* jobSystem)
    : _sysMan(sysMan),
    _jobSystem(jobSystem)
{
}

QueryInterpreterV2::~QueryInterpreterV2() {
}

// TODO: change
//       This is using RVO at best, making a copy in the worst case
QueryStatus QueryInterpreterV2::execute(const InterpreterContext& ctxt,
                                        std::string_view query,
                                        std::string_view graphName) {
    const Profile profile {"QueryInterpreterV2::execute"};

    const QueryCallbacks& callbacks = ctxt.getQueryCallbacks();

    const TimePoint start = Clock::now();
    QueryStatus status = executeImpl(ctxt, query, graphName);
    const TimePoint end = Clock::now();

    status.setTotalTime(end - start);

    if (!status.isOk()) {
        callbacks.onError(status);
    }

    callbacks.onEnd(status.getTotalTime().count());

    return status;
}

QueryStatus QueryInterpreterV2::executeImpl(const InterpreterContext& ctxt,
                                            std::string_view query,
                                            std::string_view graphName) {

    const QueryCallbacks& callbacks = ctxt.getQueryCallbacks();
    callbacks.onBegin();

    auto txRes = _sysMan->openTransaction(graphName,
                                          ctxt.getCommitHash(),
                                          ctxt.getChangeID());
    if (!txRes) {
        switch (txRes.error().getType()) {
            case ChangeErrorType::GRAPH_NOT_FOUND: {
                return QueryStatus(QueryStatus::Status::GRAPH_NOT_FOUND);
            }
            break;
            case ChangeErrorType::CHANGE_NOT_FOUND: {
                return QueryStatus(QueryStatus::Status::CHANGE_NOT_FOUND);
            }
            break;
            default: {
                return QueryStatus(QueryStatus::Status::COMMIT_NOT_FOUND);
            }
            break;
        }
    }

    const GraphView view = txRes->viewGraph();

    // Parsing query
    CypherAST ast(*ctxt.getProcedures(), query);
    CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const CompilerException& e) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PARSE_ERROR,
                            "Unknown exception occurred");
    }

    // Analyze query
    CypherAnalyzer analyzer(&ast, view);
    try {
        analyzer.analyze();
    } catch (const CompilerException& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::ANALYZE_ERROR,
                            "Unknown exception occurred");
    }

    // Generate plan graph
    PlanGraphGenerator planGen(ast, view);
    try {
        planGen.generate(ast.queries().front());
    } catch (const CompilerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            "Unknown exception occurred");
    }

    PlanGraph& planGraph = planGen.getPlanGraph();

    // Optimize plan graph
    PlanOptimizer planOpt(&planGraph);
    try {
        planOpt.optimize();
    } catch (const CompilerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            "Unknown exception occurred");
    }

    // Generate pipeline
    LocalMemory* mem = ctxt.getLocalMemory();
    PipelineV2 pipeline;
    PipelineGenerator pipelineGen(&planGraph,
                                  view,
                                  &pipeline,
                                  mem,
                                  *ctxt.getProcedures(),
                                  ctxt.getQueryCallbacks());
    try {
        pipelineGen.generate();
    } catch (const CompilerException& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::PLAN_ERROR,
                            "Unknown exception occurred");
    }

    // Execute pipeline
    ExecutionContext execCtxt(_sysMan, view);
    execCtxt.setTransaction(&txRes.value());
    execCtxt.setGraphName(graphName);
    execCtxt.setJobSystem(_jobSystem);
    execCtxt.setProcedures(ctxt.getProcedures());

    PipelineExecutor executor(&pipeline, &execCtxt);
    try {
        const Dataframe* outDf = pipeline.getOutputDataframe();
        if (outDf) {
            callbacks.onOutputHeader(outDf);
        }
        executor.execute();
    } catch (const PipelineException& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR, e.what());
    } catch (const VersionControlException& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR, e.what());
    } catch (const std::exception& e) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR,
                            std::string("Unexpected exception: ") + e.what());
    } catch (...) {
        return QueryStatus(QueryStatus::Status::EXEC_ERROR,
                            "Unknown exception occurred");
    }

    return QueryStatus(QueryStatus::Status::OK);
}
