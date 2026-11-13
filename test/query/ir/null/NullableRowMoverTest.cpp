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

// Pair p of a cross product reads inner row p % 5: a step starting at pair 3 ends the
// first tile, runs two whole tiles and stops two rows into the fourth
TEST_F(NullableRowMoverTest, tilesANullableColumnAcrossRuns) {
    const ColumnOptVector<int64_t> tile {1, std::nullopt, 3, std::nullopt, 5};

    ColumnOptVector<int64_t> output {9, 9};

    const NLBroadcastFunction tileFunction = NLExecutor::selectOptTileFunction(NLChunkKind::Int64);
    tileFunction(&tile, tile.size(), 3, 14, &output);

    ASSERT_EQ(output.size(), 14);
    EXPECT_EQ(output.getNullCount(), 6);

    for (size_t row = 0; row < output.size(); row++) {
        EXPECT_EQ(output.get(row), tile.get((3 + row) % tile.size())) << "row " << row;
    }

    tileFunction(&tile, tile.size(), 1, 3, &output);

    ASSERT_EQ(output.size(), 3);
    EXPECT_FALSE(output.isValid(0));
    EXPECT_EQ(output.get(1), std::optional<int64_t> {3});
    EXPECT_FALSE(output.isValid(2));
}

TEST_F(NullableRowMoverTest, tilesAnEmptyColumnAsNulls) {
    const ColumnOptVector<int64_t> tile;

    ColumnOptVector<int64_t> output {9, 9};

    const NLBroadcastFunction tileFunction = NLExecutor::selectOptTileFunction(NLChunkKind::Int64);
    tileFunction(&tile, tile.size(), 0, 4, &output);

    ASSERT_EQ(output.size(), 4);
    EXPECT_EQ(output.getNullCount(), 4);
}

TEST_F(NullableRowMoverTest, copiesAndAppendsANullableRange) {
    const ColumnOptVector<int64_t> input {1, std::nullopt, 3, 4};

    ColumnOptVector<int64_t> copied {9, 9, 9, 9};

    const NLCopyFunction copy = NLExecutor::selectOptCopyFunction(NLChunkKind::Int64);
    copy(&input, 1, 2, &copied);

    ASSERT_EQ(copied.size(), 2);
    EXPECT_FALSE(copied.isValid(0));
    EXPECT_EQ(copied.get(1), std::optional<int64_t> {3});

    ColumnOptVector<int64_t> buffer {7};

    const NLAppendFunction append = NLExecutor::selectOptAppendFunction(NLChunkKind::Int64);
    append(&input, &buffer);

    ASSERT_EQ(buffer.size(), 5);
    EXPECT_EQ(buffer.get(0), std::optional<int64_t> {7});

    for (size_t row = 0; row < input.size(); row++) {
        EXPECT_EQ(buffer.get(row + 1), input.get(row)) << "row " << row;
    }
}

// Rows 1, 2 and 4 are null and take the three fills in turn; a fill may itself be null
TEST_F(NullableRowMoverTest, fillsNullRowsInRuns) {
    const ColumnOptVector<int64_t> values {1, std::nullopt, std::nullopt, 4, std::nullopt, 6};
    const ColumnVector<CustomBool> nullRows {CustomBool {false},
                                             CustomBool {true},
                                             CustomBool {true},
                                             CustomBool {false},
                                             CustomBool {true},
                                             CustomBool {false}};
    const ColumnOptVector<int64_t> fills {20, std::nullopt, 50};

    ColumnOptVector<int64_t> result {9};

    const NLFillNullRowsFn fill = NLExecutor::selectFillNullRows(NLChunkKind::Int64);
    fill(&result, &values, &nullRows, &fills);

    const ColumnOptVector<int64_t> expected {1, 20, std::nullopt, 4, 50, 6};

    ASSERT_EQ(result.size(), expected.size());

    for (size_t row = 0; row < expected.size(); row++) {
        EXPECT_EQ(result.get(row), expected.get(row)) << "row " << row;
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
