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
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads one list cell (a ColumnVector<ListView> row) as a vector of strings; collect
// produces homogeneous lists, so every element is read as its string_view.
void readStringList(const ListView& view, std::vector<std::string>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(std::string(element.getAs<std::string_view>()));
    }
}

// Reads one list cell as a vector of int64s.
void readInt64List(const ListView& view, std::vector<int64_t>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(element.getAs<int64_t>());
    }
}

// Collects the (team, [names]) rows a grouped nl.collect emits: a nullable string key
// chunk and a per-group list cell chunk (ColumnVector<ListView>). The list order is
// kept (append order); the row set is captured for an order-independent assert.
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
            readStringList(listRaw[row], names);

            _rows.push_back({key, names});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the ([names]) rows an ungrouped nl.collect emits: a single list cell chunk.
class StringListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<std::string> names;
            readStringList(listRaw[row], names);
            _rows.push_back(names);
        }
    }

    const std::vector<std::vector<std::string>>& rows() const { return _rows; }

private:
    std::vector<std::vector<std::string>> _rows;
};

// Collects the (team, [scores]) rows a grouped nl.collect over an Int64 value emits.
class KeyedInt64ListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<int64_t>>;

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

            std::vector<int64_t> scores;
            readInt64List(listRaw[row], scores);

            _rows.push_back({key, scores});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, name) rows a grouped nl.unwind emits: two nullable string chunks
// (the repeated key and the unwound element).
class KeyedStringValueSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::optional<std::string>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(keys->size(), values->size());

        const auto& keyRaw = keys->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            std::optional<std::string> value;
            if (valueRaw[row]) {
                value = std::string(*valueRaw[row]);
            }

            _rows.push_back({key, value});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (name) rows an ungrouped nl.unwind emits: a single nullable string chunk.
class StringValueSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& valueRaw = values->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> value;
            if (valueRaw[row]) {
                value = std::string(*valueRaw[row]);
            }

            _rows.push_back(value);
        }
    }

    void sortedRows(std::vector<std::optional<std::string>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    size_t rowCount() const { return _rows.size(); }

private:
    std::vector<std::optional<std::string>> _rows;
};

// Collects the (team, score) rows a grouped nl.unwind over an Int64 value emits.
class KeyedInt64ValueSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(keys->size(), values->size());

        const auto& keyRaw = keys->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            _rows.push_back({key, valueRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// MATCH (a) WITH a.team AS team, collect(a.name) AS names RETURN team, names.
constexpr const char* collectNamesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %names = db.collect(%team, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gteam, %names) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a) WITH collect(a.name) AS names RETURN names: the ungrouped (keys 0) form.
constexpr const char* collectNamesKeylessProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %names = db.collect(%name) keys 0 : (!db.column<none>) -> !db.column<!storage.list<none>>
  db.output(%names) : !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a) WITH a.team AS team, collect(a.score) AS scores RETURN team, scores: the
// Int64 value path.
constexpr const char* collectScoresProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores = db.collect(%team, %score) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gteam, %scores) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a) WITH a.team AS team, collect(a.name) AS names UNWIND names AS name
// RETURN team, name.
constexpr const char* unwindNamesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %uname = db.unwind_collect(%team, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)
  db.output(%gteam, %uname) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// MATCH (a) WITH collect(a.name) AS names UNWIND names AS name RETURN name: the
// ungrouped (keys 0) round trip.
constexpr const char* unwindNamesKeylessProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %uname = db.unwind_collect(%name) keys 0 : (!db.column<none>) -> !db.column<none>
  db.output(%uname) : !db.column<none>
  return
}
)mlir";

// MATCH (a) WITH a.team AS team, collect(a.score) AS scores UNWIND scores AS score
// RETURN team, score: the Int64 unwind path.
constexpr const char* unwindScoresProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %uscore = db.unwind_collect(%team, %score) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)
  db.output(%gteam, %uscore) : !db.column<none>, !db.column<none>
  return
}
)mlir";

}

class CollectTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Six nodes grouped by "team" (String), each with an optional "name" (String) and
    // "score" (Int64):
    //   n0 red   name=Ann  score=10
    //   n1 red   name=Bob  score=20
    //   n2 blue  name=Cara score=100
    //   n3 blue  (no name, no score)   -> a null value inside the blue group, dropped
    //   n4 (none) name=Zoe score=5     -> a null grouping key
    //   n5 green (no name, no score)   -> a group whose values are all null (empty list)
    //
    // So collect(name) per team is red=[Ann, Bob], blue=[Cara], null=[Zoe], green=[],
    // and the group first-seen order (red, blue, null, green) fixes the within-group
    // element order. This one fixture exercises grouping, per-group null-value drop, a
    // null grouping key, and an all-null (empty-list) group.
    std::unique_ptr<Graph> buildCollectGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID red1 = builder.addNode(labelset);
        const NodeID red2 = builder.addNode(labelset);
        const NodeID blue1 = builder.addNode(labelset);
        const NodeID blue2 = builder.addNode(labelset);
        const NodeID lone = builder.addNode(labelset);
        const NodeID green1 = builder.addNode(labelset);

        builder.addNodeProperty<types::String>(red1, teamID, "red");
        builder.addNodeProperty<types::String>(red2, teamID, "red");
        builder.addNodeProperty<types::String>(blue1, teamID, "blue");
        builder.addNodeProperty<types::String>(blue2, teamID, "blue");
        // lone carries no team, so its grouping key is null
        builder.addNodeProperty<types::String>(green1, teamID, "green");

        builder.addNodeProperty<types::String>(red1, nameID, "Ann");
        builder.addNodeProperty<types::String>(red2, nameID, "Bob");
        builder.addNodeProperty<types::String>(blue1, nameID, "Cara");
        // blue2 carries no name -> dropped from blue's list
        builder.addNodeProperty<types::String>(lone, nameID, "Zoe");
        // green1 carries no name -> green's list is empty

        builder.addNodeProperty<types::Int64>(red1, scoreID, 10);
        builder.addNodeProperty<types::Int64>(red2, scoreID, 20);
        builder.addNodeProperty<types::Int64>(blue1, scoreID, 100);
        builder.addNodeProperty<types::Int64>(lone, scoreID, 5);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // The collect graph's schema (team / name / score) but no nodes, so a collect
    // lowers (the property names resolve) yet gathers no row.
    std::unique_ptr<Graph> buildEmptyCollectGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreatePropertyType("team", ValueType::String);
        metadata.getOrCreatePropertyType("name", ValueType::String);
        metadata.getOrCreatePropertyType("score", ValueType::Int64);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs the
    // lowered nl function against the graph view. The chunk size is exposed so a test
    // can force the collect and unwind drains to span chunk boundaries.
    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

// db.collect groups the nodes by team and gathers each group's names into a list. red
// keeps both names, blue drops blue2's null name, the team-less node forms a null-key
// group, and green (all names null) emits an empty list.
TEST_F(CollectTest, collectGroupsNamesPerTeam) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    runLoweredProgram(collectNamesProgram, reader.getView(), sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {std::nullopt, {"Zoe"}},
        {"blue", {"Cara"}},
        {"green", {}},
        {"red", {"Ann", "Bob"}},
    };
    EXPECT_EQ(rows, expected);
}

// An ungrouped collect gathers every present name into one list, one row.
TEST_F(CollectTest, collectKeylessGathersAllNames) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    StringListSink sink;
    runLoweredProgram(collectNamesKeylessProgram, reader.getView(), sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    const std::vector<std::string> expected {"Ann", "Bob", "Cara", "Zoe"};
    EXPECT_EQ(sink.rows().front(), expected);
}

// db.collect over an Int64 value column gathers each group's scores.
TEST_F(CollectTest, collectGroupsScoresPerTeam) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedInt64ListSink sink;
    runLoweredProgram(collectScoresProgram, reader.getView(), sink);

    std::vector<KeyedInt64ListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ListSink::Row> expected {
        {std::nullopt, {5}},
        {"blue", {100}},
        {"green", {}},
        {"red", {10, 20}},
    };
    EXPECT_EQ(rows, expected);
}

// db.unwind_collect re-emits one (team, name) row per collected element. green's empty
// list contributes no row; the null-key group's key repeats onto its element.
TEST_F(CollectTest, unwindGroupedReemitsNamesPerElement) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringValueSink sink;
    runLoweredProgram(unwindNamesProgram, reader.getView(), sink);

    std::vector<KeyedStringValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringValueSink::Row> expected {
        {std::nullopt, "Zoe"},
        {"blue", "Cara"},
        {"red", "Ann"},
        {"red", "Bob"},
    };
    EXPECT_EQ(rows, expected);
}

// The ungrouped collect -> unwind round trip returns every present name, one per row.
TEST_F(CollectTest, unwindKeylessRoundTripNames) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    StringValueSink sink;
    runLoweredProgram(unwindNamesKeylessProgram, reader.getView(), sink);

    std::vector<std::optional<std::string>> rows;
    sink.sortedRows(rows);

    const std::vector<std::optional<std::string>> expected {"Ann", "Bob", "Cara", "Zoe"};
    EXPECT_EQ(rows, expected);
}

// db.unwind_collect over an Int64 value re-emits one (team, score) row per element.
TEST_F(CollectTest, unwindGroupedReemitsScoresPerElement) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedInt64ValueSink sink;
    runLoweredProgram(unwindScoresProgram, reader.getView(), sink);

    std::vector<KeyedInt64ValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ValueSink::Row> expected {
        {std::nullopt, 5},
        {"blue", 100},
        {"red", 10},
        {"red", 20},
    };
    EXPECT_EQ(rows, expected);
}

// The collect emit loop re-chunks the groups: run below the group count, it slices the
// later groups at a non-zero offset, but the result set is unchanged.
TEST_F(CollectTest, collectSpansEmitChunks) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    runLoweredProgram(collectNamesProgram, reader.getView(), sink, /*chunkSize=*/1);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {std::nullopt, {"Zoe"}},
        {"blue", {"Cara"}},
        {"green", {}},
        {"red", {"Ann", "Bob"}},
    };
    EXPECT_EQ(rows, expected);
}

// The unwind emit loop re-chunks (group, element) pairs: at chunk size one, red's two
// elements land in separate chunks, so the cursor advances within a group across
// chunks - the result set is unchanged.
TEST_F(CollectTest, unwindSpansEmitChunks) {
    auto graph = buildCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringValueSink sink;
    runLoweredProgram(unwindNamesProgram, reader.getView(), sink, /*chunkSize=*/1);

    std::vector<KeyedStringValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringValueSink::Row> expected {
        {std::nullopt, "Zoe"},
        {"blue", "Cara"},
        {"red", "Ann"},
        {"red", "Bob"},
    };
    EXPECT_EQ(rows, expected);
}

// A grouped collect over a graph with no nodes folds no row, so it emits no group.
TEST_F(CollectTest, collectGroupedEmptyGraphEmitsNothing) {
    auto graph = buildEmptyCollectGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedStringListSink sink;
    runLoweredProgram(collectNamesProgram, reader.getView(), sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_TRUE(rows.empty());
}
