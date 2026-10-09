#include "ParameterBindingTest.h"

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "CompilerException.h"
#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "ParameterMap.h"
#include "StorageDialect.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"

using namespace db;
using namespace turing::test;

ParameterBindingTest::ParameterBindingTest() {
}

ParameterBindingTest::~ParameterBindingTest() {
}

void ParameterBindingTest::initialize() {
    _jobSystem = std::make_unique<JobSystem>();
    _jobSystem->init();

    _graph = Graph::create();
    SimpleGraph::createSimpleGraph(_graph.get());
}

void ParameterBindingTest::terminate() {
    _jobSystem->terminate();
}

void ParameterBindingTest::execute(const char* programText, const ParameterMap* parameters, StringRowSink& sink) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(module);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    LocalMemory memory;
    DBDialectInterpreter interpreter(*module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE, nullptr, nullptr, nullptr, nullptr, parameters);
    interpreter.run();
}

void ParameterBindingTest::expectRows(const char* programText, const ParameterMap& parameters, const std::vector<StringRowSink::Row>& expected) {
    StringRowSink sink;
    execute(programText, &parameters, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

void ParameterBindingTest::expectRowCount(const char* programText, const ParameterMap& parameters, size_t expected) {
    StringRowSink sink;
    execute(programText, &parameters, sink);

    EXPECT_EQ(sink.getRows().size(), expected);
}

void ParameterBindingTest::expectError(const char* programText, const ParameterMap* parameters, std::string_view reason) {
    StringRowSink sink;
    std::string error;

    try {
        execute(programText, parameters, sink);
    } catch (const CompilerException& e) {
        error = e.what();
    }

    ASSERT_FALSE(error.empty()) << "accepted: " << programText;
    EXPECT_NE(error.find(reason), std::string::npos) << error;
}
