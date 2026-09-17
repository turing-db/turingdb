// AFL++ / stdin fuzzing harness for the TuringDB query engine.
// Sets up a DB with SimpleGraph data and runs the v3 stages:
//   parse → analyze → codegen → lower and execute
//
// Unlike QueryInterpreterV3, this harness does NOT catch FatalException
// or bioassert failures — they crash the process so AFL reports them.
// Only what the engine throws for malformed input is caught: the
// CompilerException of the parser and analyzer, and the plain
// TuringException codegen and execution reject a query with.

#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "SystemAccessor.h"
#include "LocalMemory.h"

#include "CypherParser.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "NLSystemContext.h"
#include "StorageDialect.h"

#include "ProcedureContext.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CompilerException.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace turing::test;

namespace {

class DiscardedOutputSink : public db::NLOutputSink {
public:
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override {}
};

std::unique_ptr<TuringTestEnv> g_env;
const char* g_graphName = "fuzzdb";

void initOnce() {
    if (g_env) {
        return;
    }

    g_env = TuringTestEnv::create(fs::Path("/tmp/fuzz_query_engine"));
    db::SystemAccessor system = g_env->getSystemManager().accessUnique();
    db::Graph* graph = system.createGraph(g_graphName);
    db::SimpleGraph::createSimpleGraph(graph);
}

int fuzzOne(const char* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    const std::string_view query(data, size);

    db::SystemManager& sysMan = g_env->getSystemManager();
    db::SystemAccessor system = sysMan.accessShared();

    auto txRes = system.openTransaction(g_graphName, db::CommitHash::head(), db::ChangeID::head());
    if (!txRes) {
        return 0;
    }

    const db::GraphView view = txRes->viewGraph();

    db::NLSystemContext systemContext;
    systemContext.setSystemManager(&sysMan);
    systemContext.setAccessor(&system);
    systemContext.setTransaction(&txRes.value());
    systemContext.setGraphName(g_graphName);

    db::CypherAST ast(system.getProcedures(), query);
    db::CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const db::CompilerException&) {
        return 0;
    }

    db::CypherAnalyzer analyzer(&ast, view);
    try {
        analyzer.analyze();
    } catch (const db::CompilerException&) {
        return 0;
    }

    if (ast.queries().empty()) {
        return 0;
    }

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    mlir::OpBuilder builder(&context);
    mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
    mlir::ModuleOp module = owningModule.get();

    const mlir::db::DBPassContext passContext {&view};

    db::DBProgramGenerator generator(&module, nullptr, passContext);
    try {
        generator.generate(&ast);
    } catch (const FatalException&) {
        throw;
    } catch (const TuringException&) {
        return 0;
    }

    db::LocalMemory mem;

    db::ProcedureContext procedureContext;
    procedureContext.setGraph(system.getGraph(g_graphName));
    procedureContext.setGraphView(&view);
    procedureContext.setTransaction(&txRes.value());
    procedureContext.setProcedures(system.getProcedures());
    procedureContext.setListBuffer(&mem.listBuffer());

    DiscardedOutputSink sink;
    db::DBDialectInterpreter interpreter(module,
                                         &view,
                                         &sink,
                                         &mem,
                                         db::ChunkConfig::CHUNK_SIZE,
                                         nullptr,
                                         nullptr,
                                         &procedureContext,
                                         &systemContext);
    try {
        interpreter.run();
    } catch (const FatalException&) {
        throw;
    } catch (const TuringException&) {
        return 0;
    }

    return 0;
}

}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    initOnce();

    __AFL_INIT();
    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        const int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    return EXIT_SUCCESS;
}

#else
int main(int argc, char** argv) {
    initOnce();

    std::string input;
    char buf[4096];
    while (const size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
