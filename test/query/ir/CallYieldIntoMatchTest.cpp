#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <utility>
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
#include "columns/ColumnOptVector.h"
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

// Collects the single projected string property column a query emits.
class NameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& namesRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(namesRaw[rowIndex].has_value());
            _names.push_back(std::string(*namesRaw[rowIndex]));
        }
    }

    void sortedNames(std::vector<std::string>& names) const {
        names = _names;
        std::sort(names.begin(), names.end());
    }

private:
    std::vector<std::string> _names;
};

// Collects the (node, node) rows a yielded node column and a matched one form.
class NodePairSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* yielded = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* matched = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        ASSERT_NE(yielded, nullptr);
        ASSERT_NE(matched, nullptr);
        ASSERT_EQ(yielded->size(), matched->size());

        const auto& yieldedRaw = yielded->getRaw();
        const auto& matchedRaw = matched->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(yieldedRaw[rowIndex].getValue(), matchedRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

struct FounderNodesData : public ProcedureData {};

// test.founderNodes() YIELD person: emits node IDs 0 and 1 - Remy and Adam on the
// simpledb fixture - so a MATCH after the call can read the nodes it yielded.
void founderNodesExecuteImpl(ProcedureState* procedureState) {
    FounderNodesData& data = procedureState->data<FounderNodesData>();

    auto* people = static_cast<ColumnVector<NodeID>*>(data.getReturnColumn(0));
    people->clear();
    people->push_back(NodeID {0});
    people->push_back(NodeID {1});

    procedureState->finish();
}

void founderNodesExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        founderNodesExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* founderNodesAlloc() {
    return new FounderNodesData();
}

void founderNodesDealloc(ProcedureData* data) {
    delete data;
}

struct FirstNodesData : public ProcedureData {};

// test.firstNodes(count) YIELD person: emits the node IDs below the constant count, so a
// source call reads a literal argument to decide the rows a following MATCH consumes. A
// count of 2 gives Remy and Adam on the simpledb fixture.
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

struct WantedAgeData : public ProcedureData {};

// test.wantedAge() YIELD wanted: emits the single row 32 - the age Remy and Adam carry
// on the simpledb fixture - so a MATCH's WHERE can compare a property against it.
void wantedAgeExecuteImpl(ProcedureState* procedureState) {
    WantedAgeData& data = procedureState->data<WantedAgeData>();

    auto* wanted = static_cast<ColumnVector<types::Int64::Primitive>*>(data.getReturnColumn(0));
    wanted->clear();
    wanted->push_back(32);

    procedureState->finish();
}

void wantedAgeExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        wantedAgeExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* wantedAgeAlloc() {
    return new WantedAgeData();
}

void wantedAgeDealloc(ProcedureData* data) {
    delete data;
}

}

class CallYieldIntoMatchTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        // The rows a following MATCH consumes must be deterministic, so the yielded
        // nodes and the yielded age come from test-local source procedures.
        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* founderNodesProcedure = new Procedure("founderNodes");
        founderNodesProcedure->setAllocCallback(&founderNodesAlloc);
        founderNodesProcedure->setDeallocCallback(&founderNodesDealloc);
        founderNodesProcedure->setExecuteCallback(&founderNodesExecute);
        founderNodesProcedure->addReturnValue("person", ProcedureType::NODE);
        testNamespace->addProcedure(founderNodesProcedure);

        Procedure* firstNodesProcedure = new Procedure("firstNodes");
        firstNodesProcedure->setAllocCallback(&firstNodesAlloc);
        firstNodesProcedure->setDeallocCallback(&firstNodesDealloc);
        firstNodesProcedure->setExecuteCallback(&firstNodesExecute);
        firstNodesProcedure->addArgument("count", ProcedureType::INT64);
        firstNodesProcedure->addReturnValue("person", ProcedureType::NODE);
        testNamespace->addProcedure(firstNodesProcedure);

        Procedure* wantedAgeProcedure = new Procedure("wantedAge");
        wantedAgeProcedure->setAllocCallback(&wantedAgeAlloc);
        wantedAgeProcedure->setDeallocCallback(&wantedAgeDealloc);
        wantedAgeProcedure->setExecuteCallback(&wantedAgeExecute);
        wantedAgeProcedure->addReturnValue("wanted", ProcedureType::INT64);
        testNamespace->addProcedure(wantedAgeProcedure);

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // Runs a Cypher query the whole way down against the simpledb fixture: parsed and
    // analyzed against the test's own procedure registry, generated into db dialect by
    // DBProgramGenerator, lowered and executed - the path a CALL takes from the query
    // text.
    void runQuery(const char* queryText, NLOutputSink& sink) {
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
        procedureContext.setChunkSize(ChunkConfig::CHUNK_SIZE);
        procedureContext.setListBuffer(&memory.listBuffer());

        DBDialectInterpreter interpreter(moduleOp,
                                         &view,
                                         &sink,
                                         &memory,
                                         ChunkConfig::CHUNK_SIZE,
                                         /*writeBuffer=*/nullptr,
                                         /*metadataBuilder=*/nullptr,
                                         &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
};

// CALL test.founderNodes() YIELD person MATCH (person)-->(m) RETURN m.name: the MATCH
// reads the yielded nodes, so it expands the out-edges of Remy and Adam alone - Adam,
// Ghosts, Computers and Eighties from Remy, Remy, Bio and Cooking from Adam.
TEST_F(CallYieldIntoMatchTest, matchReadsTheYieldedNodes) {
    NameSink sink;
    runQuery("CALL test.founderNodes() YIELD person MATCH (person)-->(m) RETURN m.name", sink);

    std::vector<std::string> names;
    sink.sortedNames(names);

    const std::vector<std::string> expected {
        "Adam", "Bio", "Computers", "Cooking", "Eighties", "Ghosts", "Remy"};
    EXPECT_EQ(names, expected);
}

// CALL test.firstNodes(2) YIELD person MATCH (person)-->(m) RETURN person, m: a source
// call whose only argument is a constant, and a projection of the yielded node beside the
// matched one - so the yielded column has to reach the RETURN row-aligned with what the
// expansion found. Remy's four out-edges and Adam's three make the seven rows.
TEST_F(CallYieldIntoMatchTest, projectsTheYieldedNodeBesideTheMatchedOne) {
    NodePairSink sink;
    runQuery("CALL test.firstNodes(2) YIELD person MATCH (person)-->(m) RETURN person, m", sink);

    std::vector<NodePairSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<NodePairSink::Row> expected {{0, 1}, {0, 2}, {0, 3}, {0, 6}, {1, 0}, {1, 4}, {1, 5}};
    EXPECT_EQ(rows, expected);
}

// CALL test.wantedAge() YIELD wanted MATCH (n) WHERE n.age = wanted RETURN n.name: the
// MATCH's WHERE compares a property against the yielded value, and only Remy and Adam
// carry an age of 32.
TEST_F(CallYieldIntoMatchTest, matchWhereReadsTheYieldedValue) {
    NameSink sink;
    runQuery("CALL test.wantedAge() YIELD wanted MATCH (n) WHERE n.age = wanted RETURN n.name", sink);

    std::vector<std::string> names;
    sink.sortedNames(names);

    const std::vector<std::string> expected {"Adam", "Remy"};
    EXPECT_EQ(names, expected);
}
