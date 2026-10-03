#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "TuringException.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "SimpleGraph.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (dob, [names]) rows a grouped nl.collect emits: a nullable string key
// chunk and a per-group list cell chunk (ColumnVector<ListView>).
class KeyedStringListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<std::string>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            std::vector<std::string> names;
            for (const ListElementView& element : listRaw[row]) {
                names.push_back(std::string(element.getAs<std::string_view>()));
            }

            _rows.push_back({key, names});
        }
    }

    // Fills the rows with each group's names sorted, then the rows themselves sorted:
    // both the group order and the append order within a group follow the
    // datapart-major scan, so the assert stays order-independent.
    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        for (Row& row : rows) {
            std::sort(row.second.begin(), row.second.end());
        }

        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// WITH n.dob AS dob, collect(n.name) AS names RETURN dob, names, in the shape
// DBLowering emits: the grouping key and the collected value are two property fetches
// of the same scan chunk, so the columns nl.collect_update folds are row-aligned. The
// control for the misaligned program below.
constexpr const char* alignedCollectProgram = R"mlir(
func.func @main() {
  %buf = nl.collect_buffer keys 1
  %dobType = nl.get_property_type("dob")
  %nameType = nl.get_property_type("name")
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %dob = nl.get_node_properties(%a, %dobType) : !nl.chunk<!storage.nullable<!storage.string>>
    %name = nl.get_node_properties(%a, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.collect_update %buf, (%dob, %name) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %groups = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>>
  nl.for %gdob, %names in %groups : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>> {
    nl.output(%gdob, %names) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>
  }
  func.return
}
)mlir";

// The same collect, but the grouping key is cut down by an nl.filter while the
// collected value column keeps every row of the step, so the two columns
// nl.collect_update folds are no longer row-aligned. On simpledb's first data part the
// step carries 7 nodes and only Remy and Adam have age 32, so the key column holds 2
// rows against the value column's 7.
constexpr const char* misalignedCollectProgram = R"mlir(
func.func @main() {
  %buf = nl.collect_buffer keys 1
  %dobType = nl.get_property_type("dob")
  %nameType = nl.get_property_type("name")
  %ageType = nl.get_property_type("age")
  %thirtyTwo = nl.constant(32 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %dob = nl.get_node_properties(%a, %dobType) : !nl.chunk<!storage.nullable<!storage.string>>
    %name = nl.get_node_properties(%a, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    %age = nl.get_node_properties(%a, %ageType) : !nl.chunk<!storage.nullable<i64>>
    %mask = nl.eq %age, %thirtyTwo : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %fewerDobs = nl.filter %mask, (%dob) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
    nl.collect_update %buf, (%fewerDobs, %name) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %groups = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>>
  nl.for %gdob, %names in %groups : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>> {
    nl.output(%gdob, %names) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>
  }
  func.return
}
)mlir";

}

class CollectRowAlignmentTest : public TuringTest {
protected:
    void runProgram(const char* programText, const GraphView& view, NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }
};

// The control: with the key and the value column row-aligned, the collect groups
// simpledb's nodes by dob. Only the four Persons carrying a dob form a named group;
// every other node falls into the null-key group.
TEST_F(CollectRowAlignmentTest, collectGroupsNamesPerDob) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    runProgram(alignedCollectProgram, reader.getView(), sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {std::nullopt, {"Animals",
                        "Bio",
                        "Computers",
                        "Cooking",
                        "Cyrus",
                        "Doruk",
                        "Eighties",
                        "Ghosts",
                        "Gym",
                        "JiuJitsu",
                        "Martina",
                        "Padel",
                        "Suhas",
                        "Travel"}},
        {"18/01", {"Remy"}},
        {"18/08", {"Adam"}},
        {"24/07", {"Maxime"}},
        {"28/05", {"Luc"}},
    };
    EXPECT_EQ(rows, expected);
}

// REVIEW.md #11: collectFold bounds its loop by the value column's row count but
// indexes the group assignments, which runCollectUpdate sized from the first key
// column. A value column longer than the keys walks past that vector and uses the
// garbage it reads to index the per-group position lists - a wrong group at best, a
// heap write out of range at worst. Row alignment is nl.collect_update's precondition,
// so a step that breaks it must be rejected, not folded.
TEST_F(CollectRowAlignmentTest, collectUpdateRejectsValueColumnLongerThanKeys) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    EXPECT_THROW(runProgram(misalignedCollectProgram, reader.getView(), sink), TuringException);
}
