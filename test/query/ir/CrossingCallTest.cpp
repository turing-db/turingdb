#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <tuple>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "ProcedureState.h"
#include "ProcUtils.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "SimpleGraph.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (node, node, value) rows two node columns and an integer one form, in the
// order the projection names them.
class HopValueSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, types::Int64::Primitive>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* people = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* targets = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* values = dynamic_cast<const ColumnVector<types::Int64::Primitive>*>(chunks[2]);
        ASSERT_NE(people, nullptr);
        ASSERT_NE(targets, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(people->size(), targets->size());
        ASSERT_EQ(people->size(), values->size());

        const auto& peopleRaw = people->getRaw();
        const auto& targetsRaw = targets->getRaw();
        const auto& valuesRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(peopleRaw[rowIndex].getValue(),
                               targetsRaw[rowIndex].getValue(),
                               valuesRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (node, node, node) rows three node columns form, in the order the
// projection names them.
class NodeTripleSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* people = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* hops = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* targets = dynamic_cast<const ColumnVector<NodeID>*>(chunks[2]);
        ASSERT_NE(people, nullptr);
        ASSERT_NE(hops, nullptr);
        ASSERT_NE(targets, nullptr);
        ASSERT_EQ(people->size(), hops->size());
        ASSERT_EQ(people->size(), targets->size());

        const auto& peopleRaw = people->getRaw();
        const auto& hopsRaw = hops->getRaw();
        const auto& targetsRaw = targets->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(peopleRaw[rowIndex].getValue(),
                               hopsRaw[rowIndex].getValue(),
                               targetsRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

struct FirstNodesData : public ProcedureData {};

// test.firstNodes(count) YIELD person: the node IDs below the constant count, so the hop
// chain after the call starts from the rows it yielded. A count of 2 gives Remy and Adam
// on the simpledb fixture.
void firstNodesExecuteImpl(ProcedureState* procedureState) {
    FirstNodesData& data = procedureState->data<FirstNodesData>();

    const types::Int64::Primitive count
        = ProcUtils::constArg<types::Int64::Primitive>(data.getInputColumn(0),
                                                       "test.firstNodes: count must be a constant int");

    auto* people = static_cast<ColumnVector<NodeID>*>(data.getReturnColumn(0));
    people->clear();

    for (types::Int64::Primitive nodeIndex = 0; nodeIndex < count; nodeIndex++) {
        people->push_back(NodeID {static_cast<NodeID::Type>(nodeIndex)});
    }

    procedureState->finish();
}

void firstNodesExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        firstNodesExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* firstNodesAlloc() {
    return new FirstNodesData();
}

void firstNodesDealloc(ProcedureData* data) {
    delete data;
}

struct OffsetPairData : public ProcedureData {};

// test.offsetPair(base) YIELD offset: emits base and base + 1. Its only argument is a
// constant, so a call of it standing after a MATCH reads none of the rows in flight.
void offsetPairExecuteImpl(ProcedureState* procedureState) {
    OffsetPairData& data = procedureState->data<OffsetPairData>();

    const types::Int64::Primitive base
        = ProcUtils::constArg<types::Int64::Primitive>(data.getInputColumn(0),
                                                       "test.offsetPair: base must be a constant int");

    auto* offsets = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    offsets->clear();
    offsets->push_back(base);
    offsets->push_back(base + 1);

    procedureState->finish();
}

void offsetPairExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        offsetPairExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* offsetPairAlloc() {
    return new OffsetPairData();
}

void offsetPairDealloc(ProcedureData* data) {
    delete data;
}

struct DoubleNodeIDData : public IndexedProcedureData {};

// test.doubleNodeID(nodeIDs) YIELD doubled: twice each input node ID, beside the input row
// it came from - a call that does read the rows it is driven with.
void doubleNodeIDExecuteImpl(ProcedureState* procedureState) {
    DoubleNodeIDData& data = procedureState->data<DoubleNodeIDData>();

    const auto* nodeIDs = static_cast<const ColumnVector<NodeID>*>(data.getInputColumn(0));
    auto* doubled = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    ColumnIndices* indices = data.indices();

    doubled->clear();

    const auto& nodeIDsRaw = nodeIDs->getRaw();
    for (size_t inputRow = 0; inputRow < nodeIDsRaw.size(); inputRow++) {
        doubled->push_back(2 * static_cast<types::Int64::Primitive>(nodeIDsRaw[inputRow].getValue()));

        if (indices) {
            indices->push_back(inputRow);
        }
    }

    procedureState->finish();
}

void doubleNodeIDExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        doubleNodeIDExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* doubleNodeIDAlloc() {
    return new DoubleNodeIDData();
}

void doubleNodeIDDealloc(ProcedureData* data) {
    delete data;
}

}

class CrossingCallTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        // The rows the hop chain consumes must be deterministic, so the nodes it starts
        // from and the values the second call emits come from test-local procedures.
        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* firstNodesProcedure = new Procedure("firstNodes");
        firstNodesProcedure->setAllocCallback(&firstNodesAlloc);
        firstNodesProcedure->setDeallocCallback(&firstNodesDealloc);
        firstNodesProcedure->setExecuteCallback(&firstNodesExecute);
        firstNodesProcedure->addConstantArgument("count", ProcedureType::INT64);
        firstNodesProcedure->addReturnValue("person", ProcedureType::NODE);
        testNamespace->addProcedure(firstNodesProcedure);

        Procedure* offsetPairProcedure = new Procedure("offsetPair");
        offsetPairProcedure->setAllocCallback(&offsetPairAlloc);
        offsetPairProcedure->setDeallocCallback(&offsetPairDealloc);
        offsetPairProcedure->setExecuteCallback(&offsetPairExecute);
        offsetPairProcedure->addConstantArgument("base", ProcedureType::INT64);
        offsetPairProcedure->addReturnValue("offset", ProcedureType::INT64);
        testNamespace->addProcedure(offsetPairProcedure);

        Procedure* doubleNodeIDProcedure = new Procedure("doubleNodeID");
        doubleNodeIDProcedure->setAllocCallback(&doubleNodeIDAlloc);
        doubleNodeIDProcedure->setDeallocCallback(&doubleNodeIDDealloc);
        doubleNodeIDProcedure->setExecuteCallback(&doubleNodeIDExecute);
        doubleNodeIDProcedure->setHasIndices(true);
        doubleNodeIDProcedure->addArgument("nodeIDs", ProcedureType::NODE);
        doubleNodeIDProcedure->addReturnValue("doubled", ProcedureType::INT64);
        testNamespace->addProcedure(doubleNodeIDProcedure);

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // Runs a Cypher query the whole way down against the simpledb fixture: parsed and
    // analyzed against the test's own procedure registry, generated into db dialect by
    // DBProgramGenerator, lowered and executed.
    void runQuery(const char* queryText, NLOutputSink& sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

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
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;

        ProcedureContext procedureContext;
        procedureContext.setGraphView(&view);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        DBDialectInterpreter interpreter(moduleOp,
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
    std::unique_ptr<Graph> _graph;
};

// CALL test.firstNodes(2) YIELD person MATCH (person)-->(m) MATCH (m)-->(z)
// CALL test.offsetPair(17) YIELD offset RETURN person, z, offset: a call opens the
// dataflow, two MATCH clauses hop from the column it yielded, and a second call reads
// none of those rows - so it crosses with them, and everything the chain bound has to
// survive being paired with the rows it emits.
//
// The two hops from Remy and Adam give eight (person, z) rows - Remy reaches himself
// twice, through Adam and through Ghosts - and the crossed call pairs its two rows with
// every one of them.
TEST_F(CrossingCallTest, crossesASourceCallAfterAHopChain) {
    HopValueSink sink;
    runQuery("CALL test.firstNodes(2) YIELD person "
             "MATCH (person)-->(m) "
             "MATCH (m)-->(z) "
             "CALL test.offsetPair(17) YIELD offset "
             "RETURN person, z, offset",
             sink,
             /*chunkSize=*/2);

    std::vector<HopValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<HopValueSink::Row> expected {
        {0, 0, 17},
        {0, 0, 17},
        {0, 0, 18},
        {0, 0, 18},
        {0, 4, 17},
        {0, 4, 18},
        {0, 5, 17},
        {0, 5, 18},
        {1, 1, 17},
        {1, 1, 18},
        {1, 2, 17},
        {1, 2, 18},
        {1, 3, 17},
        {1, 3, 18},
        {1, 6, 17},
        {1, 6, 18},
    };
    EXPECT_EQ(rows, expected);
}

// The same shape with a second call that does read the chain: test.doubleNodeID takes the
// z the last hop bound, so the call joins the rows in flight rather than crossing them and
// the eight the chain found stay eight.
TEST_F(CrossingCallTest, chainsACallReadingTheHopChainTail) {
    HopValueSink sink;
    runQuery("CALL test.firstNodes(2) YIELD person "
             "MATCH (person)-->(m) "
             "MATCH (m)-->(z) "
             "CALL test.doubleNodeID(z) YIELD doubled "
             "RETURN person, z, doubled",
             sink,
             /*chunkSize=*/2);

    std::vector<HopValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<HopValueSink::Row> expected {
        {0, 0, 0},
        {0, 0, 0},
        {0, 4, 8},
        {0, 5, 10},
        {1, 1, 2},
        {1, 2, 4},
        {1, 3, 6},
        {1, 6, 12},
    };
    EXPECT_EQ(rows, expected);
}

// CALL test.firstNodes(2) YIELD person AS a MATCH (a)-->(m)
// CALL test.firstNodes(3) YIELD person AS z MATCH (m)-->(z) RETURN a, m, z: the second
// call binds the name the last hop ends on, so that hop runs with both its endpoints
// already bound - an edge between two columns rather than a traversal into a free one.
//
// The crossed call pairs Remy, Adam and Computers with each of the seven rows the first
// hop found, and the second hop keeps the four of those pairs a real edge stands behind.
TEST_F(CrossingCallTest, joinsAHopOntoTheNodesACallYielded) {
    NodeTripleSink sink;
    runQuery("CALL test.firstNodes(2) YIELD person AS a "
             "MATCH (a)-->(m) "
             "CALL test.firstNodes(3) YIELD person AS z "
             "MATCH (m)-->(z) "
             "RETURN a, m, z",
             sink,
             /*chunkSize=*/2);

    std::vector<NodeTripleSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<NodeTripleSink::Row> expected {
        {0, 1, 0},
        {0, 6, 0},
        {1, 0, 1},
        {1, 0, 2},
    };
    EXPECT_EQ(rows, expected);
}

// The same query returning every bound column rather than naming them: the wildcard has
// to project what the two calls yielded and what the hops bound, and only those - the
// anonymous edges the pattern walked are not variables the query bound.
TEST_F(CrossingCallTest, wildcardReturnsTheJoinedColumns) {
    NodeTripleSink sink;
    runQuery("CALL test.firstNodes(2) YIELD person AS a "
             "MATCH (a)-->(m) "
             "CALL test.firstNodes(3) YIELD person AS z "
             "MATCH (m)-->(z) "
             "RETURN *",
             sink,
             /*chunkSize=*/2);

    std::vector<NodeTripleSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<NodeTripleSink::Row> expected {
        {0, 1, 0},
        {0, 6, 0},
        {1, 0, 1},
        {1, 0, 2},
    };
    EXPECT_EQ(rows, expected);
}
