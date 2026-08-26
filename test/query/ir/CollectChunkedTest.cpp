#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
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
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using Names = std::vector<std::string>;
using Values = std::vector<int64_t>;
using NodeIDs = std::vector<uint64_t>;
using OptInt64 = std::optional<int64_t>;
using OptDouble = std::optional<double>;

// Reads one list cell as a vector of strings.
void readStringList(const ListView& view, Names& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(std::string(element.getAs<std::string_view>()));
    }
}

// Reads one list cell as a vector of int64s.
void readInt64List(const ListView& view, Values& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(element.getAs<int64_t>());
    }
}

// Reads one list cell as a vector of node ids, asserting the node tag.
void readNodeIDList(const ListView& view, NodeIDs& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::NodeID);
        out.push_back(element.getAs<NodeID>().getValue());
    }
}

// Collects the (team, [values]) rows a grouped collect emits, reading each list cell
// with the reader for the collected element type.
template <typename Element, void (*Reader)(const ListView&, std::vector<Element>&)>
class KeyedListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<Element>>;

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

            std::vector<Element> values;
            Reader(listRaw[row], values);

            _rows.push_back({key, values});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

using KeyedValueListSink = KeyedListSink<int64_t, readInt64List>;
using KeyedNameListSink = KeyedListSink<std::string, readStringList>;
using KeyedNodeListSink = KeyedListSink<uint64_t, readNodeIDList>;

// Collects the single [values] row an ungrouped collect emits.
class ValueListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            Values values;
            readInt64List(listRaw[row], values);

            _rows.push_back(values);
        }
    }

    const std::vector<Values>& rows() const { return _rows; }

private:
    std::vector<Values> _rows;
};

// Collects the (team, [values], count, sum, min, max) rows a collect carrying the four
// plain reductions emits.
class KeyedListAndReductionsSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, Values, uint64_t, OptInt64, OptInt64, OptInt64>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 6u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[3]);
        const auto* minima = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[4]);
        const auto* maxima = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[5]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_NE(minima, nullptr);
        ASSERT_NE(maxima, nullptr);

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& countRaw = counts->getRaw();
        const auto& sumRaw = sums->getRaw();
        const auto& minimumRaw = minima->getRaw();
        const auto& maximumRaw = maxima->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Values values;
            readInt64List(listRaw[row], values);

            _rows.push_back({key, values, countRaw[row], sumRaw[row], minimumRaw[row], maximumRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, [values], count_distinct, sum_distinct, avg_distinct) rows a
// collect carrying the three distinct reductions emits.
class KeyedListAndDistinctReductionsSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, Values, uint64_t, OptInt64, OptDouble>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 5u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[3]);
        const auto* averages = dynamic_cast<const ColumnOptVector<double>*>(chunks[4]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_NE(averages, nullptr);

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& countRaw = counts->getRaw();
        const auto& sumRaw = sums->getRaw();
        const auto& averageRaw = averages->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Values values;
            readInt64List(listRaw[row], values);

            _rows.push_back({key, values, countRaw[row], sumRaw[row], averageRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// MATCH (a) RETURN a.team, collect(DISTINCT a.score).
constexpr const char* distinctScoresProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores = db.collect(%team, %score) keys 1 distinct : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gteam, %scores) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

// The same collect without the flag: the bag the distinct form deduplicates.
constexpr const char* everyScoreProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores = db.collect(%team, %score) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gteam, %scores) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a) RETURN collect(DISTINCT a.score): one group over the whole scan, so every
// chunk folds into the same distinct tally.
constexpr const char* distinctScoresKeylessProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %scores = db.collect(%score) keys 0 distinct : (!db.column<none>) -> !db.column<!storage.list<none>>
  db.output(%scores) : !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a) RETURN a.team, collect(DISTINCT a.name): the String fold, which keys a
// group on the bytes of the value rather than on a scalar.
constexpr const char* distinctNamesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %names = db.collect(%team, %name) keys 1 distinct : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gteam, %names) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

// MATCH (a)-->(b) RETURN a.team, collect(DISTINCT b): the entity fold. A node reached
// over two edges is one element, and at chunk size one the two edges are two steps.
constexpr const char* distinctTargetsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %src, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %team = db.get_node_properties(%src, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %targets = db.collect(%team, %b) keys 1 distinct : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.list<!storage.node_id>>)
  db.output(%gteam, %targets) : !db.column<none>, !db.column<!storage.list<!storage.node_id>>
  return
}
)mlir";

// MATCH (a) RETURN a.team, collect(a.score), count(a.score), sum(a.score),
// min(a.score), max(a.score): four accumulators folded over the same groups as the list.
constexpr const char* reductionsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores, %n, %total, %low, %high = db.collect(%team, %score, %score, %score, %score, %score) keys 1 aggregates [count, sum, min, max] : (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>)
  db.output(%gteam, %scores, %n, %total, %low, %high) : !db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>
  return
}
)mlir";

// The distinct reductions beside a plain list: each keeps a tally of the values its
// group has already charged, which every chunk of the group folds into.
constexpr const char* distinctReductionsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores, %n, %total, %mean = db.collect(%team, %score, %score, %score, %score) keys 1 aggregates [count_distinct, sum_distinct, avg_distinct] : (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>)
  db.output(%gteam, %scores, %n, %total, %mean) : !db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>
  return
}
)mlir";

// The distinct reductions beside a distinct list: the list and the reductions dedupe
// the same column through tallies of their own, so both must survive the chunking.
constexpr const char* distinctReductionsAndListProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %scores, %n, %total, %mean = db.collect(%team, %score, %score, %score, %score) keys 1 aggregates [count_distinct, sum_distinct, avg_distinct] distinct : (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>)
  db.output(%gteam, %scores, %n, %total, %mean) : !db.column<none>, !db.column<!storage.list<none>>, !db.column<none>, !db.column<none>, !db.column<none>
  return
}
)mlir";

}

// collect(DISTINCT x) and the reductions a collect carries both accumulate in state
// hoisted above the producing loop, which nl.collect_update folds one chunk at a time.
// Every test here runs its program at a chunk size that splits the fixture and at the
// production one, against a single expectation: a tally or an accumulator that did not
// survive a chunk boundary parts the two.
class CollectChunkedTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Seven nodes over three teams, each with an optional "name" (String) and "score"
    // (Int64), and four edges of two types:
    //   n0 red   name=Ann  score=10   -> n2 (KNOWS), -> n2 (LIKES), so n2 twice
    //   n1 red   name=Bob  score=20   -> n5 (KNOWS)
    //   n2 blue  name=Cara score=100  -> n0 (KNOWS)
    //   n3 red   name=Ann  score=10   repeats red's name and score three rows on
    //   n4 blue  name=Dee  score=100  repeats blue's score, -> n0 (KNOWS)
    //   n5 red   name=Eve  score=30
    //   n6 green (no name, no score)  the group holding no value at all
    //
    // Every repeat is several rows away from what it repeats, so a chunk size of two or
    // three puts the two occurrences in different chunks.
    std::unique_ptr<Graph> buildRepeatingGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;
        const EdgeTypeID knows = metadata.getOrCreateEdgeType("KNOWS");
        const EdgeTypeID likes = metadata.getOrCreateEdgeType("LIKES");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID red1 = builder.addNode(labelset);
        const NodeID red2 = builder.addNode(labelset);
        const NodeID blue1 = builder.addNode(labelset);
        const NodeID red3 = builder.addNode(labelset);
        const NodeID blue2 = builder.addNode(labelset);
        const NodeID red4 = builder.addNode(labelset);
        const NodeID green1 = builder.addNode(labelset);

        builder.addNodeProperty<types::String>(red1, teamID, "red");
        builder.addNodeProperty<types::String>(red2, teamID, "red");
        builder.addNodeProperty<types::String>(blue1, teamID, "blue");
        builder.addNodeProperty<types::String>(red3, teamID, "red");
        builder.addNodeProperty<types::String>(blue2, teamID, "blue");
        builder.addNodeProperty<types::String>(red4, teamID, "red");
        builder.addNodeProperty<types::String>(green1, teamID, "green");

        builder.addNodeProperty<types::String>(red1, nameID, "Ann");
        builder.addNodeProperty<types::String>(red2, nameID, "Bob");
        builder.addNodeProperty<types::String>(blue1, nameID, "Cara");
        builder.addNodeProperty<types::String>(red3, nameID, "Ann");
        builder.addNodeProperty<types::String>(blue2, nameID, "Dee");
        builder.addNodeProperty<types::String>(red4, nameID, "Eve");

        builder.addNodeProperty<types::Int64>(red1, scoreID, 10);
        builder.addNodeProperty<types::Int64>(red2, scoreID, 20);
        builder.addNodeProperty<types::Int64>(blue1, scoreID, 100);
        builder.addNodeProperty<types::Int64>(red3, scoreID, 10);
        builder.addNodeProperty<types::Int64>(blue2, scoreID, 100);
        builder.addNodeProperty<types::Int64>(red4, scoreID, 30);

        builder.addEdge(knows, red1, blue1);
        builder.addEdge(likes, red1, blue1);
        builder.addEdge(knows, red2, red4);
        builder.addEdge(knows, blue1, red1);
        builder.addEdge(knows, blue2, red1);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs the lowered
    // function against the graph view at this chunk size.
    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize) {
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

    // One row per chunk, then two and three rows per chunk - each splitting the seven
    // nodes and the five edges differently - and last the production size, which holds
    // the whole fixture in one chunk.
    const std::vector<size_t> _chunkSizes {1, 2, 3, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(CollectChunkedTest, collectsDistinctScoresPerGroup) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedValueListSink::Row> expected {
        {"blue", {100}},
        {"green", {}},
        {"red", {10, 20, 30}},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedValueListSink sink;
        runLoweredProgram(distinctScoresProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedValueListSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, collectsEveryScorePerGroup) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedValueListSink::Row> expected {
        {"blue", {100, 100}},
        {"green", {}},
        {"red", {10, 20, 10, 30}},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedValueListSink sink;
        runLoweredProgram(everyScoreProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedValueListSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, collectsDistinctScoresOverTheWholeScan) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const Values expected {10, 20, 100, 30};

    for (const size_t chunkSize : _chunkSizes) {
        ValueListSink sink;
        runLoweredProgram(distinctScoresKeylessProgram, reader.getView(), sink, chunkSize);

        ASSERT_EQ(sink.rows().size(), 1u) << "at chunk size " << chunkSize;
        EXPECT_EQ(sink.rows().front(), expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, collectsDistinctNamesPerGroup) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedNameListSink::Row> expected {
        {"blue", {"Cara", "Dee"}},
        {"green", {}},
        {"red", {"Ann", "Bob", "Eve"}},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedNameListSink sink;
        runLoweredProgram(distinctNamesProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedNameListSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, collectsDistinctTargetsPerGroup) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedNodeListSink::Row> expected {
        {"blue", {0}},
        {"red", {2, 5}},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedNodeListSink sink;
        runLoweredProgram(distinctTargetsProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedNodeListSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, reducesEveryPlainKindBesideTheList) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedListAndReductionsSink::Row> expected {
        {"blue", {100, 100}, 2, 200, 100, 100},
        {"green", {}, 0, 0, std::nullopt, std::nullopt},
        {"red", {10, 20, 10, 30}, 4, 70, 10, 30},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedListAndReductionsSink sink;
        runLoweredProgram(reductionsProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedListAndReductionsSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, reducesDistinctValuesBesideTheList) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedListAndDistinctReductionsSink::Row> expected {
        {"blue", {100, 100}, 1, 100, 100.0},
        {"green", {}, 0, 0, std::nullopt},
        {"red", {10, 20, 10, 30}, 3, 60, 20.0},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedListAndDistinctReductionsSink sink;
        runLoweredProgram(distinctReductionsProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedListAndDistinctReductionsSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}

TEST_F(CollectChunkedTest, reducesDistinctValuesBesideADistinctList) {
    auto graph = buildRepeatingGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const std::vector<KeyedListAndDistinctReductionsSink::Row> expected {
        {"blue", {100}, 1, 100, 100.0},
        {"green", {}, 0, 0, std::nullopt},
        {"red", {10, 20, 30}, 3, 60, 20.0},
    };

    for (const size_t chunkSize : _chunkSizes) {
        KeyedListAndDistinctReductionsSink sink;
        runLoweredProgram(distinctReductionsAndListProgram, reader.getView(), sink, chunkSize);

        std::vector<KeyedListAndDistinctReductionsSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "at chunk size " << chunkSize;
    }
}
