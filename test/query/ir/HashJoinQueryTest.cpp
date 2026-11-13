#include "HashJoinQueryTest.h"

#include <algorithm>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/Support/raw_ostream.h"

#include "Graph.h"
#include "ProcedureContext.h"
#include "SimpleGraph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

using namespace db;
using namespace turing::test;

HashJoinQueryTest::HashJoinQueryTest() {
}

HashJoinQueryTest::~HashJoinQueryTest() {
}

void HashJoinQueryTest::initialize() {
    _graph = Graph::create();
    SimpleGraph::createSimpleGraph(_graph.get());

    _procedures.init();

    _context.getOrLoadDialect<mlir::func::FuncDialect>();
    _context.getOrLoadDialect<mlir::storage::Storage>();
    _context.getOrLoadDialect<mlir::db::DB>();
    _context.getOrLoadDialect<mlir::nl::NL>();
}

void HashJoinQueryTest::dbModule(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    generate(query, view, module.get(), true);
}

void HashJoinQueryTest::dbProgram(std::string_view query, std::string& program) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    render(module.get(), program);
}

void HashJoinQueryTest::nlProgram(std::string_view query, std::string& program) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    generate(query, view, module.get(), true);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    lower(view, module.get(), nlModule.get());

    render(nlModule.get(), program);
}

void HashJoinQueryTest::runQuery(std::string_view query, StringRowSink& sink, bool forcesJoin) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    generate(query, view, module.get(), forcesJoin);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    lower(view, module.get(), nlModule.get());

    LocalMemory memory;

    ProcedureContext procedureContext;
    procedureContext.setGraph(_graph.get());
    procedureContext.setGraphView(&view);
    procedureContext.setProcedures(&_procedures);
    procedureContext.setChunkSize(ChunkConfig::CHUNK_SIZE);
    procedureContext.setListBuffer(&memory.listBuffer());
    procedureContext.setStringBuffer(&memory.stringBuffer());

    NLInterpreter interpreter(*nlModule,
                              &view,
                              &sink,
                              &memory,
                              ChunkConfig::CHUNK_SIZE,
                              nullptr,
                              nullptr,
                              &procedureContext);
    interpreter.run();
}

void HashJoinQueryTest::expectCount(std::string_view query, size_t expected) {
    StringRowSink sink;
    runQuery(query, sink);

    const Rows expectedRows {{std::to_string(expected)}};
    EXPECT_EQ(sink.getRows(), expectedRows) << "query: " << query;
}

void HashJoinQueryTest::expectRows(std::string_view query, const Rows& expected) {
    StringRowSink sink;
    runQuery(query, sink);

    EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
}

void HashJoinQueryTest::expectJoinedRows(std::string_view query, const Rows& expected) {
    Rows sortedExpected = expected;
    std::sort(sortedExpected.begin(), sortedExpected.end());

    StringRowSink joined;
    runQuery(query, joined);
    Rows joinedRows;
    joined.sortedRows(joinedRows);
    EXPECT_EQ(joinedRows, sortedExpected) << "query: " << query;

    StringRowSink product;
    runQuery(query, product, false);
    Rows productRows;
    product.sortedRows(productRows);
    EXPECT_EQ(productRows, sortedExpected) << "query: " << query;
}

bool HashJoinQueryTest::contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

void HashJoinQueryTest::generate(std::string_view query,
                                 const GraphView& view,
                                 mlir::ModuleOp module,
                                 bool forcesJoin) {
    CypherAST ast(&_procedures, query);

    CypherParser parser(&ast);
    parser.parse(query);

    CypherAnalyzer analyzer(&ast, view);
    analyzer.analyze();

    const mlir::db::DBPassContext passContext {&view, forcesJoin, true};
    DBProgramGenerator generator(&module, nullptr, passContext);
    generator.generate(&ast);
}

void HashJoinQueryTest::lower(const GraphView& view, mlir::ModuleOp module, mlir::ModuleOp nlModule) {
    const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    DBLowering lowering(&_context, &view, &_procedures);
    lowering.lower(dbFunction, nlModule);
}

void HashJoinQueryTest::render(mlir::ModuleOp module, std::string& program) {
    program.clear();
    llvm::raw_string_ostream stream(program);

    mlir::OpPrintingFlags flags;
    flags.assumeVerified();

    module.print(stream, flags);
}
