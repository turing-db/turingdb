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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRException.h"
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

// Collects the (dob, [names]) rows the nl.collect drain emits: a nullable string key
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

// Tallies the emitted rows without reading them, for a program whose translation must
// fail before any chunk is produced. It makes no claim about the column shapes, so the
// only thing a run of the rejected module can report is that it was not rejected.
class RowCountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// WITH n.dob AS dob, collect(n.name) AS names RETURN dob, names over the Persons, in the
// shape DBLowering emits: one nl.collect_buffer, the nl.collect_update that fills it,
// and a single drain - here the per-group nl.collect. The prefix the two-drain module
// below extends, so it is also the control that the prefix itself is valid.
constexpr const char* singleDrainProgram = R"mlir(
func.func @main() {
  %buf = nl.collect_buffer keys 1
  %dobType = nl.get_property_type("dob")
  %nameType = nl.get_property_type("name")
  %persons = nl.scan_nodes_by_label(["Person"])
  nl.for %a in %persons : !nl.iter<!nl.chunk<!storage.node_id>> {
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

// The same accumulator drained twice: the per-group nl.collect and then the
// per-element nl.unwind_collect, each with its own emit loop. Both drains write through
// the one output slot the shared NLCollectState carries, so the accumulator can only
// serve one of them.
constexpr const char* twoDrainsProgram = R"mlir(
func.func @main() {
  %buf = nl.collect_buffer keys 1
  %dobType = nl.get_property_type("dob")
  %nameType = nl.get_property_type("name")
  %persons = nl.scan_nodes_by_label(["Person"])
  nl.for %a in %persons : !nl.iter<!nl.chunk<!storage.node_id>> {
    %dob = nl.get_node_properties(%a, %dobType) : !nl.chunk<!storage.nullable<!storage.string>>
    %name = nl.get_node_properties(%a, %nameType) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.collect_update %buf, (%dob, %name) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %groups = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>>
  nl.for %gdob, %names in %groups : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>> {
    nl.output(%gdob, %names) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>
  }
  %elements = nl.unwind_collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>>
  nl.for %udob, %uname in %elements : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>> {
    nl.output(%udob, %uname) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  func.return
}
)mlir";

}

class CollectDrainTest : public TuringTest {
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

// The control: a single drain on the accumulator groups the Persons by dob. Only the
// four carrying a dob form a named group; the other four fall into the null-key group.
TEST_F(CollectDrainTest, collectDrainGroupsNamesPerDob) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    runProgram(singleDrainProgram, reader.getView(), sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {std::nullopt, {"Cyrus", "Doruk", "Martina", "Suhas"}},
        {"18/01", {"Remy"}},
        {"18/08", {"Adam"}},
        {"24/07", {"Maxime"}},
        {"28/05", {"Luc"}},
    };
    EXPECT_EQ(rows, expected);
}

// REVIEW.md #12: NLCollectState holds one output slot for the drained value (and one per
// grouping key), so the second drain's translation rebinds the first drain's outputs to
// its own, incompatible column types - the nl.collect drain ends up emitting ListViews
// through the nl.unwind_collect drain's ColumnOptVector. translateCollectUpdate already
// rejects a second update on one accumulator; the drains need the same rejection, since
// nothing in NLOps.td constrains how many name a given nl.collect_buffer.
TEST_F(CollectDrainTest, rejectsTwoDrainsOnOneCollectBuffer) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowCountingSink sink;
    EXPECT_THROW(runProgram(twoDrainsProgram, reader.getView(), sink), IRException);
}
