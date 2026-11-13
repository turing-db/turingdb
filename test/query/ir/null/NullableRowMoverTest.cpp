#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "IRTestRows.h"
#include "NLDialect.h"
#include "NLExecutor.h"
#include "NLInterpreter.h"
#include "NLProgram.h"
#include "NLValueStore.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The interests read as nullable IDs, then the same rows once the null literal has been cast
// to nodes, which leaves every ID invalid
const char* const entityToNullableProgram = R"mlir(
func.func @main() {
  %null = nl.constant("" : !storage.nullable<none>)
  %interests = nl.scan_nodes_by_label(["Interest"])
  nl.for %a in %interests : !nl.iter<!nl.chunk<!storage.node_id>> {
    %laid = nl.broadcast_constant %null, %a : (!nl.chunk<!storage.nullable<none>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<none>>
    %absent = nl.checked_cast %laid : (!nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.node_id>
    %ids = nl.to_nullable %a : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<ui64>>
    %none = nl.to_nullable %absent : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<ui64>>
    nl.output(%ids, %none) : !nl.chunk<!storage.nullable<ui64>>, !nl.chunk<!storage.nullable<ui64>>
  }
  func.return
}
)mlir";

// Doruk, the one node labelled Sales, read in a chunk of one row of its own and laid out over
// the interests: his name, and the age he lacks
const char* const singleRowBroadcastProgram = R"mlir(
func.func @main() {
  %name = nl.get_property_type("name")
  %age = nl.get_property_type("age")
  %sales = nl.scan_nodes_by_label(["Sales"])
  nl.for %s in %sales : !nl.iter<!nl.chunk<!storage.node_id>> {
    %names = nl.get_node_properties(%s, %name) : !nl.chunk<!storage.nullable<!storage.string>>
    %ages = nl.get_node_properties(%s, %age) : !nl.chunk<!storage.nullable<i64>>
    %interests = nl.scan_nodes_by_label(["Interest"])
    nl.for %a in %interests : !nl.iter<!nl.chunk<!storage.node_id>> {
      %laidNames = nl.broadcast_constant %names, %a : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<!storage.string>>
      %laidAges = nl.broadcast_constant %ages, %a : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<i64>>
      nl.output(%laidNames, %laidAges) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>
    }
  }
  func.return
}
)mlir";

constexpr size_t SIMPLEDB_INTEREST_COUNT = 10;

}

// simpledb has 10 interests; of its 8 people, Remy and Adam have an age.
class NullableRowMoverTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    void runNLProgram(const char* programText, size_t chunkSize, RowSink& sink) {
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = parse(programText);
        ASSERT_TRUE(nlModule);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &reader.getView(), &sink, &memory, chunkSize);
        interpreter.run();
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(NullableRowMoverTest, readsAnEntityAsANullableID) {
    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        RowSink sink;
        runNLProgram(entityToNullableProgram, chunkSize, sink);

        const Rows expected = {
            {"2", "null"},
            {"3", "null"},
            {"4", "null"},
            {"5", "null"},
            {"6", "null"},
            {"7", "null"},
            {"10", "null"},
            {"13", "null"},
            {"14", "null"},
            {"16", "null"},
        };
        EXPECT_EQ(sink.rows(), expected) << "chunk size " << chunkSize;
    }
}

TEST_F(NullableRowMoverTest, laysOutAOneRowColumn) {
    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        RowSink sink;
        runNLProgram(singleRowBroadcastProgram, chunkSize, sink);

        EXPECT_EQ(sink.rows(), Rows(SIMPLEDB_INTEREST_COUNT, Row {"Doruk", "null"})) << "chunk size " << chunkSize;
    }
}

// 6000 strings built per row outgrow the accumulator's store, which is compacted while
// six of the eight rows hold a null
TEST_F(NullableRowMoverTest, compactsANullableStringAccumulator) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, reduce(s = '', x IN range(1, 6000) | "
               "CASE WHEN p.age IS NULL THEN null ELSE p.name + toString(x) END)",
               {
                   {"Remy", "Remy6000"},
                   {"Adam", "Adam6000"},
                   {"Maxime", "null"},
                   {"Luc", "null"},
                   {"Martina", "null"},
                   {"Suhas", "null"},
                   {"Cyrus", "null"},
                   {"Doruk", "null"},
               });
}

TEST_F(NullableRowMoverTest, ownsTheNullableRowsFromTheFirstOneOn) {
    std::string text = "abc";
    const std::string_view view = text;

    ColumnOptVector<std::string_view> column;
    column.push_back(view);
    column.pushNull();
    column.push_back(view);

    NLValueStore store;
    store.ownColumnRows(&column, 1);
    text = "xyz";

    EXPECT_EQ(column.getValue(0), "xyz");
    EXPECT_FALSE(column.isValid(1));
    ASSERT_TRUE(column.isValid(2));
    EXPECT_EQ(column.getValue(2), "abc");
    EXPECT_EQ(store.getValueCount(), 1);
}

TEST_F(NullableRowMoverTest, ownsThePresentValueOfANullableConstant) {
    std::string text = "abc";
    const std::string_view view = text;

    ColumnConst<std::optional<std::string_view>> present;
    present.set(view);

    ColumnConst<std::optional<std::string_view>> absent;
    absent.set(std::nullopt);

    NLValueStore store;
    store.ownColumnRows(&present, 0);
    store.ownColumnRows(&absent, 0);
    text = "xyz";

    ASSERT_TRUE(present.isValid(0));
    EXPECT_EQ(present.getValue(0), "abc");
    EXPECT_FALSE(absent.isValid(0));
    EXPECT_EQ(store.getValueCount(), 1);
}

TEST_F(NullableRowMoverTest, laysOutAnEmptyOneRowColumnAsNulls) {
    const ColumnOptVector<int64_t> value;

    ColumnOptVector<int64_t> output;
    output.push_back(7);

    const NLBroadcastConstantFunction fill = NLExecutor::selectConstantBroadcast(NLChunkKind::Int64, &value);
    fill(&value, 3, &output);

    ASSERT_EQ(output.size(), 3);
    EXPECT_EQ(output.getNullCount(), 3);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
