#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "JobSystem.h"
#include "ProcedureContext.h"
#include "ProcedureManager.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Whether a commit string is the one db.history marks as the commit being read. Only the
// head row carries the marker, so this is what tells the walk's direction apart.
bool isHeadCommit(const std::string& commit) {
    return commit.ends_with("(HEAD)");
}

// Collects the (id, name) rows a schema procedure emits: an ID chunk of the kind the
// schema numbers its entries with, and a borrowed-string chunk of their names. db.labels
// and db.edgeTypes have that same shape over two different ID types, so the sink is
// written once over the ID type rather than twice.
template <typename IDType>
class IDNameSink : public NLOutputSink {
public:
    using Row = std::pair<typename IDType::Type, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ids = dynamic_cast<const ColumnVector<IDType>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[1]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(names, nullptr);
        ASSERT_EQ(ids->size(), names->size());

        _calls++;

        const auto& idRaw = ids->getRaw();
        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(idRaw[rowIndex].getValue(), std::string(nameRaw[rowIndex]));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    size_t getCalls() const { return _calls; }

private:
    std::vector<Row> _rows;
    size_t _calls {0};
};

// Collects the (id, name, value type) rows db.propertyTypes emits. The value type arrives
// as a chunk of the storage enum, and is kept here under its name so a mismatch reads as
// the type it is rather than as the byte it is stored as.
class PropertyTypeSink : public NLOutputSink {
public:
    using Row = std::tuple<PropertyTypeID::Type, std::string, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* ids = dynamic_cast<const ColumnVector<PropertyTypeID>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[1]);
        const auto* valueTypes = dynamic_cast<const ColumnVector<ValueType>*>(chunks[2]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(valueTypes, nullptr);
        ASSERT_EQ(ids->size(), names->size());
        ASSERT_EQ(ids->size(), valueTypes->size());

        const auto& idRaw = ids->getRaw();
        const auto& nameRaw = names->getRaw();
        const auto& valueTypeRaw = valueTypes->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(idRaw[rowIndex].getValue(),
                               std::string(nameRaw[rowIndex]),
                               std::string(ValueTypeName::value(valueTypeRaw[rowIndex])));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the single borrowed-string column a call yielding one name emits, so a test can
// assert the return values it did not yield are left unbound.
class NameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& raw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(raw[rowIndex]);
        }
    }

    void sortedRows(std::vector<std::string>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::string> _rows;
};

// Collects the (commit, node count, edge count, part count) rows db.history emits: an
// owned-string chunk and three unsigned counters, plus how many chunks they arrived in.
// The rows are kept in the order they were emitted, since walking the chain head-first is
// part of what the procedure produces.
class HistorySink : public NLOutputSink {
public:
    struct Row {
        std::string _commit;
        types::UInt64::Primitive _nodeCount {0};
        types::UInt64::Primitive _edgeCount {0};
        types::UInt64::Primitive _partCount {0};
    };

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 4u);

        const auto* commits = dynamic_cast<const ColumnVector<std::string>*>(chunks[0]);
        const auto* nodeCounts = dynamic_cast<const ColumnVector<types::UInt64::Primitive>*>(chunks[1]);
        const auto* edgeCounts = dynamic_cast<const ColumnVector<types::UInt64::Primitive>*>(chunks[2]);
        const auto* partCounts = dynamic_cast<const ColumnVector<types::UInt64::Primitive>*>(chunks[3]);
        ASSERT_NE(commits, nullptr);
        ASSERT_NE(nodeCounts, nullptr);
        ASSERT_NE(edgeCounts, nullptr);
        ASSERT_NE(partCounts, nullptr);
        ASSERT_EQ(commits->size(), nodeCounts->size());
        ASSERT_EQ(commits->size(), edgeCounts->size());
        ASSERT_EQ(commits->size(), partCounts->size());

        _calls++;

        const auto& commitRaw = commits->getRaw();
        const auto& nodeCountRaw = nodeCounts->getRaw();
        const auto& edgeCountRaw = edgeCounts->getRaw();
        const auto& partCountRaw = partCounts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back(Row {commitRaw[rowIndex],
                                 nodeCountRaw[rowIndex],
                                 edgeCountRaw[rowIndex],
                                 partCountRaw[rowIndex]});
        }
    }

    const std::vector<Row>& rows() const { return _rows; }
    size_t getCalls() const { return _calls; }

private:
    std::vector<Row> _rows;
    size_t _calls {0};
};

// Counts emissions without materializing them, so a call that finds nothing to walk can be
// shown to emit nothing at all rather than an empty chunk.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _calls++;
        _rows += rowCount;
    }

    size_t getCalls() const { return _calls; }
    size_t getRows() const { return _rows; }

private:
    size_t _calls {0};
    size_t _rows {0};
};

}

class MLIRDBProceduresTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        // Only the real db namespace: every procedure under test here is a registered
        // one, so nothing test-local is added to the registry.
        _procedures.init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // A three-label, two-edge-type, three-property-type schema built over two commits: the
    // first carries the whole schema and three nodes joined by two edges, the second adds
    // two more nodes and one more edge. So each schema procedure has several entries to
    // walk, and the chain db.history walks is the graph's root commit plus those two.
    std::unique_ptr<Graph> buildSchemaGraph() {
        auto graph = Graph::create();

        const LabelSet labelset = LabelSet::fromList({0});

        {
            auto change = graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();
            auto& metadata = builder.getMetadata();

            metadata.getOrCreateLabel("Person");
            metadata.getOrCreateLabel("Employee");
            metadata.getOrCreateLabel("Manager");

            metadata.getOrCreateEdgeType("KNOWS");
            metadata.getOrCreateEdgeType("MANAGES");

            metadata.getOrCreatePropertyType("name", ValueType::String);
            metadata.getOrCreatePropertyType("age", ValueType::Int64);
            metadata.getOrCreatePropertyType("score", ValueType::Double);

            const NodeID node0 = builder.addNode(labelset);
            const NodeID node1 = builder.addNode(labelset);
            const NodeID node2 = builder.addNode(labelset);

            builder.addEdge(0, node0, node1);
            builder.addEdge(1, node1, node2);

            const auto submitResult = change->access().submit(*_jobSystem);
            EXPECT_TRUE(submitResult);
        }

        {
            auto change = graph->newChange();
            auto* commitBuilder = change->access().getTip();
            auto& builder = commitBuilder->newBuilder();

            const NodeID node3 = builder.addNode(labelset);
            const NodeID node4 = builder.addNode(labelset);

            builder.addEdge(0, node3, node4);

            const auto submitResult = change->access().submit(*_jobSystem);
            EXPECT_TRUE(submitResult);
        }

        return graph;
    }

    // One label and one node, no edge and no property: a graph whose edge-type and
    // property-type schemas are empty, so a procedure walking them finds nothing. A label
    // is not optional - every node carries at least one.
    std::unique_ptr<Graph> buildBareGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Person");

        const LabelSet labelset = LabelSet::fromList({0});
        builder.addNode(labelset);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Runs a Cypher query the whole way down: parsed and analyzed against the procedure
    // registry, generated into db dialect by DBProgramGenerator, lowered to nl and
    // executed. This is the path a CALL takes from the query text, so it covers the
    // frontend as well as the engine. The chunk size is exposed so a test can force a
    // procedure to span chunk boundaries.
    void runQuery(const char* queryText,
                  Graph* graph,
                  const GraphView& view,
                  NLOutputSink& sink,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;

        // A procedure reads the request through this context, so it carries everything one
        // may ask for: the graph and the transaction as well as the view, since a
        // version-control procedure like db.history reads the commit being queried.
        Transaction transaction(graph->openTransaction());

        ProcedureContext procedureContext;
        procedureContext.setGraph(graph);
        procedureContext.setGraphView(&view);
        procedureContext.setTransaction(&transaction);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  chunkSize,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(MLIRDBProceduresTest, cypherLabelsYieldsEveryLabel) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The three labels of the schema, each with the ID it was numbered with, straight from
    // the query text.
    IDNameSink<LabelID> sink;
    runQuery("CALL db.labels() YIELD id, label RETURN id, label", graph.get(), reader.getView(), sink);

    std::vector<IDNameSink<LabelID>::Row> rows;
    sink.sortedRows(rows);
    const std::vector<IDNameSink<LabelID>::Row> expected {{0, "Person"}, {1, "Employee"}, {2, "Manager"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRDBProceduresTest, cypherLabelsSpansChunks) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three labels at a chunk size of two: the drive loop must run the procedure again
    // after each chunk, so the rows arrive in two chunks (2, 1) and none is lost.
    IDNameSink<LabelID> sink;
    runQuery("CALL db.labels() YIELD id, label RETURN id, label", graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/2);

    std::vector<IDNameSink<LabelID>::Row> rows;
    sink.sortedRows(rows);
    const std::vector<IDNameSink<LabelID>::Row> expected {{0, "Person"}, {1, "Employee"}, {2, "Manager"}};
    EXPECT_EQ(rows, expected);
    EXPECT_EQ(sink.getCalls(), 2u);
}

TEST_F(MLIRDBProceduresTest, cypherEdgeTypesYieldsEveryEdgeType) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The same shape as db.labels over the edge-type schema, so the call yields a chunk of
    // edge-type IDs rather than label ones.
    IDNameSink<EdgeTypeID> sink;
    runQuery("CALL db.edgeTypes() YIELD id, edgeType RETURN id, edgeType", graph.get(), reader.getView(), sink);

    std::vector<IDNameSink<EdgeTypeID>::Row> rows;
    sink.sortedRows(rows);
    const std::vector<IDNameSink<EdgeTypeID>::Row> expected {{0, "KNOWS"}, {1, "MANAGES"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRDBProceduresTest, cypherPropertyTypesYieldsEveryPropertyType) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three return values, one of them the stored value type of the property - a chunk kind
    // no scan produces, so this is the whole declared shape of the procedure coming back
    // through the query.
    PropertyTypeSink sink;
    runQuery("CALL db.propertyTypes() YIELD id, propertyType, valueType RETURN id, propertyType, valueType",
             graph.get(),
             reader.getView(),
             sink);

    std::vector<PropertyTypeSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<PropertyTypeSink::Row> expected {{0, "name", "String"},
                                                       {1, "age", "Int64"},
                                                       {2, "score", "Double"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRDBProceduresTest, cypherPropertyTypesBindsOnlyTheYieldedReturnValue) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Yielding only `propertyType` leaves the `id` and `valueType` return values unbound,
    // so the procedure fills the name column alone - and the one column it does fill is
    // neither its first nor its last.
    NameSink sink;
    runQuery("CALL db.propertyTypes() YIELD propertyType RETURN propertyType", graph.get(), reader.getView(), sink);

    std::vector<std::string> rows;
    sink.sortedRows(rows);
    const std::vector<std::string> expected {"age", "name", "score"};
    EXPECT_EQ(rows, expected);
}

TEST_F(MLIRDBProceduresTest, cypherHistoryWalksTheCommitChainHeadFirst) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The chain is the graph's root commit and the two submits over it, walked back from
    // the commit being read - so the rows come head-first: the second submit (two nodes,
    // one edge, one data part), then the first (three nodes, two edges, one part), then the
    // empty root. Each commit reports what it added, not the graph's running total, and
    // only the first row is marked as the head.
    // `commit` is a keyword of the language, so the RETURN cannot name it: the YIELD renames
    // it, and the call still binds the procedure's own return value behind that alias.
    HistorySink sink;
    runQuery("CALL db.history() YIELD commit AS commitHash, nodeCount, edgeCount, partCount "
             "RETURN commitHash, nodeCount, edgeCount, partCount",
             graph.get(),
             reader.getView(),
             sink);

    const std::vector<HistorySink::Row>& rows = sink.rows();
    ASSERT_EQ(rows.size(), 3u);

    EXPECT_TRUE(isHeadCommit(rows[0]._commit));
    EXPECT_FALSE(isHeadCommit(rows[1]._commit));
    EXPECT_FALSE(isHeadCommit(rows[2]._commit));

    EXPECT_EQ(rows[0]._nodeCount, 2u);
    EXPECT_EQ(rows[0]._edgeCount, 1u);
    EXPECT_EQ(rows[0]._partCount, 1u);

    EXPECT_EQ(rows[1]._nodeCount, 3u);
    EXPECT_EQ(rows[1]._edgeCount, 2u);
    EXPECT_EQ(rows[1]._partCount, 1u);

    EXPECT_EQ(rows[2]._nodeCount, 0u);
    EXPECT_EQ(rows[2]._edgeCount, 0u);
}

TEST_F(MLIRDBProceduresTest, cypherHistorySpansChunks) {
    auto graph = buildSchemaGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three commits at a chunk size of one: the cursor walks a single commit per call, so
    // the rows arrive in three chunks - and the chain is still walked head-first end to
    // end, which is what the cursor surviving across the chunks means.
    HistorySink sink;
    runQuery("CALL db.history() YIELD commit AS commitHash, nodeCount, edgeCount, partCount "
             "RETURN commitHash, nodeCount, edgeCount, partCount",
             graph.get(),
             reader.getView(),
             sink,
             /*chunkSize=*/1);

    const std::vector<HistorySink::Row>& rows = sink.rows();
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(sink.getCalls(), 3u);

    EXPECT_TRUE(isHeadCommit(rows[0]._commit));
    EXPECT_EQ(rows[0]._nodeCount, 2u);
    EXPECT_EQ(rows[1]._nodeCount, 3u);
    EXPECT_EQ(rows[2]._nodeCount, 0u);
}

TEST_F(MLIRDBProceduresTest, cypherEmptySchemasYieldNothing) {
    auto graph = buildBareGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The graph carries no edge and no property, so both schemas are empty: the procedure
    // finishes on its first call and the drive loop emits no chunk at all rather than an
    // empty one.
    CountingSink edgeTypeSink;
    runQuery("CALL db.edgeTypes() YIELD id, edgeType RETURN id, edgeType", graph.get(), reader.getView(), edgeTypeSink);

    EXPECT_EQ(edgeTypeSink.getRows(), 0u);
    EXPECT_EQ(edgeTypeSink.getCalls(), 0u);

    CountingSink propertyTypeSink;
    runQuery("CALL db.propertyTypes() YIELD propertyType RETURN propertyType", graph.get(),
             reader.getView(),
             propertyTypeSink);

    EXPECT_EQ(propertyTypeSink.getRows(), 0u);
    EXPECT_EQ(propertyTypeSink.getCalls(), 0u);
}
