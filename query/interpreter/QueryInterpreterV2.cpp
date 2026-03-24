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
#include "TuringException.h"

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

void QueryInterpreterV2::execute(const InterpreterContext& ctxt,
                                 QueryStatus& status,
                                 std::string_view query,
                                 std::string_view graphName) {
    const Profile profile("QueryInterpreterV2::execute");

    const QueryCallbacks* callbacks = ctxt.getQueryCallbacks();

    {
        const TimePoint start = Clock::now();
        executeImpl(ctxt, status, query, graphName);
        const TimePoint end = Clock::now();

        status.setTotalTime(end - start);
    }

    if (!status.isOk()) {
        callbacks->onError(status);
    }

    callbacks->onEnd(status.getTotalTime().count());
}

void QueryInterpreterV2::executeImpl(const InterpreterContext& ctxt,
                                     QueryStatus& status,
                                     std::string_view query,
                                     std::string_view graphName) {
    const QueryCallbacks* callbacks = ctxt.getQueryCallbacks();
    callbacks->onBegin();

    auto txRes = _sysMan->openTransaction(graphName,
                                          ctxt.getCommitHash(),
                                          ctxt.getChangeID());
    if (!txRes) {
        switch (txRes.error().getType()) {
            case ChangeErrorType::GRAPH_NOT_FOUND: {
                status.setStatus(QueryStatus::Status::GRAPH_NOT_FOUND);
                return;
            }
            break;
            case ChangeErrorType::CHANGE_NOT_FOUND: {
                status.setStatus(QueryStatus::Status::CHANGE_NOT_FOUND);
                return;
            }
            break;
            case ChangeErrorType::COMMIT_NOT_LOADED: {
                status.setStatus(QueryStatus::Status::COMMIT_NOT_LOADED);
                status.setMessage(txRes.error().fmtMessage());
                return;
            }
            break;
            default: {
                status.setStatus(QueryStatus::Status::COMMIT_NOT_FOUND);
                return;
            }
            break;
        }
    }

    const GraphView view = txRes->viewGraph();

    // Parsing query
    CypherAST ast(ctxt.getProcedures(), query);
    CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    // Analyze query
    CypherAnalyzer analyzer(&ast, view);
    try {
        analyzer.analyze();
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    // Generate plan graph
    const QueryConfig* queryConfig = ctxt.getQueryConfig();
    PlanGraphGenerator planGen(ast, view, &queryConfig->getPlanGenConfig());
    try {
        planGen.generate(ast.queries().front());
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    PlanGraph& planGraph = planGen.getPlanGraph();

    // Optimize plan graph
    PlanOptimizer planOpt(&planGraph);
    try {
        planOpt.optimize();
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    // Generate pipeline
    LocalMemory* mem = ctxt.getLocalMemory();
    PipelineV2 pipeline;
    PipelineGenerator pipelineGen(&planGraph,
                                  view,
                                  &pipeline,
                                  mem,
                                  _sysMan,
                                  ctxt.getProcedures(),
                                  ctxt.getQueryCallbacks());
    try {
        pipelineGen.generate();
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    // Execute pipeline
    ExecutionContext execCtxt(_sysMan, view);
    execCtxt.setChunkSize(queryConfig->getChunkSize());
    execCtxt.setTransaction(&txRes.value());
    execCtxt.setGraphName(graphName);
    execCtxt.setJobSystem(_jobSystem);
    execCtxt.setProcedures(ctxt.getProcedures());
    execCtxt.setExtensions(ctxt.getExtensions());
    execCtxt.setVectorDatabase(ctxt.getVectorDatabase());

    PipelineExecutor executor(&pipeline, &execCtxt);
    try {
        const Dataframe* outDf = pipeline.getOutputDataframe();
        if (outDf) {
            callbacks->onOutputHeader(outDf);
        }
        executor.execute();
    } catch (const PipelineException& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const VersionControlException& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const TuringException& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }
}
