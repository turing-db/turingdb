// AFL++ / stdin fuzzing harness for the full TuringDB query engine.
// Sets up a DB with SimpleGraph data and runs the full query pipeline:
//   parse → analyze → plan → optimize → generate pipeline → execute
//
// Unlike QueryInterpreterV2, this harness does NOT catch FatalException
// or bioassert failures — they crash the process so AFL reports them.
// Only CompilerException and PipelineException (expected user errors)
// are caught.

#include "TuringDB.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "LocalMemory.h"

#include "CypherParser.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "PlanGraphGenerator.h"
#include "PlanOptimizer.h"
#include "PipelineGenerator.h"
#include "PipelineV2.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"

#include "CompilerException.h"
#include "PipelineException.h"
#include "versioning/VersionControlException.h"
#include "versioning/Transaction.h"

#include "procedures/ProcedureManager.h"

using namespace turing::test;

static std::unique_ptr<TuringTestEnv> g_env;
static const char* g_graphName = "fuzzdb";

static void initOnce() {
    if (g_env) return;

    g_env = TuringTestEnv::create(fs::Path("/tmp/fuzz_query_engine"));
    auto* graph = g_env->getSystemManager().createGraph(g_graphName);
    db::SimpleGraph::createSimpleGraph(graph);
}

static int fuzzOne(const char* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    std::string_view query(data, size);

    db::TuringDB& db = g_env->getDB();
    db::SystemManager& sysMan = g_env->getSystemManager();
    const db::ProcedureManager* procedures = db.getProcedures();

    // Open transaction
    auto txRes = sysMan.openTransaction(g_graphName,
                                        db::CommitHash::head(),
                                        db::ChangeID::head());
    if (!txRes) {
        return 0;
    }

    const db::GraphView view = txRes->viewGraph();

    // Parse
    db::CypherAST ast(procedures, query);
    db::CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const db::CompilerException&) {
        return 0;
    }

    // Analyze
    db::CypherAnalyzer analyzer(&ast, view);
    try {
        analyzer.analyze();
    } catch (const db::CompilerException&) {
        return 0;
    }

    if (ast.queries().empty()) {
        return 0;
    }

    // Plan
    db::PlanGraphGenerator planGen(ast, view);
    try {
        planGen.generate(ast.queries().front());
    } catch (const db::CompilerException&) {
        return 0;
    }

    db::PlanGraph& planGraph = planGen.getPlanGraph();

    // Optimize
    db::PlanOptimizer planOpt(&planGraph);
    try {
        planOpt.optimize();
    } catch (const db::CompilerException&) {
        return 0;
    }

    // Generate pipeline
    db::LocalMemory mem;
    db::PipelineV2 pipeline;
    db::QueryCallbacks callbacks;
    db::PipelineGenerator pipelineGen(&planGraph,
                                      view,
                                      &pipeline,
                                      &mem,
                                      &sysMan,
                                      procedures,
                                      &callbacks);
    try {
        pipelineGen.generate();
    } catch (const db::CompilerException&) {
        return 0;
    }

    // Execute pipeline
    db::ExecutionContext execCtxt(&sysMan, view);
    execCtxt.setTransaction(&txRes.value());
    execCtxt.setGraphName(g_graphName);
    execCtxt.setJobSystem(&db.getJobSystem());
    execCtxt.setProcedures(procedures);
    execCtxt.setExtensions(db.getExtensions());
    execCtxt.setVectorDatabase(nullptr);

    db::PipelineExecutor executor(&pipeline, &execCtxt);
    try {
        executor.execute();
    } catch (const db::PipelineException&) {
        return 0;
    } catch (const db::VersionControlException&) {
        return 0;
    }

    return 0;
}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    initOnce();

    __AFL_INIT();
    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    return 0;
}

#else
int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    initOnce();

    std::string input;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
