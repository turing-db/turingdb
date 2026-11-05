#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
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
#include "columns/Column.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
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

#include "ID.h"
#include "IRTestRows.h"
#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

struct NeighbourData : public ProcedureData {};

// test.maybeNeighbour yields four rows as one chunk: Remy (0), no node, Adam (1), no node
void neighbourExecuteImpl(ProcedureState* procedureState) {
    NeighbourData& data = procedureState->data<NeighbourData>();

    ColumnNodeIDs* neighbours = static_cast<ColumnNodeIDs*>(data.getReturnColumn(0));
    neighbours->clear();
    neighbours->push_back(NodeID(0));
    neighbours->push_back(NodeID());
    neighbours->push_back(NodeID(1));
    neighbours->push_back(NodeID());

    procedureState->finish();
}

void neighbourExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        neighbourExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* neighbourAlloc() {
    return new NeighbourData();
}

void neighbourDealloc(ProcedureData* data) {
    delete data;
}

// Collects the one node column a query emits, and the type name of any chunk that
// reached the sink as something else
class NodeColumnSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* neighbours = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        if (!neighbours) {
            _otherKinds.emplace_back(chunks[0]->getTypeName());
            return;
        }

        const std::vector<NodeID>& raw = neighbours->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<NodeID>& values() const { return _values; }
    const std::vector<std::string>& otherKinds() const { return _otherKinds; }

private:
    std::vector<NodeID> _values;
    std::vector<std::string> _otherKinds;
};

}

// A procedure's NODE return value spells no node as an invalid ID, the null an OPTIONAL
// MATCH leaves, so every op reads the absent rows as it reads that null.
class CallNullableYieldTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* neighbourProcedure = new Procedure("maybeNeighbour");
        neighbourProcedure->setAllocCallback(&neighbourAlloc);
        neighbourProcedure->setDeallocCallback(&neighbourDealloc);
        neighbourProcedure->setExecuteCallback(&neighbourExecute);
        neighbourProcedure->addReturnValue("neighbour", ProcedureType::NODE);
        testNamespace->addProcedure(neighbourProcedure);

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // Runs a Cypher query the whole way down against the simpledb fixture: parsed and
    // analyzed against the test's own procedure registry, generated into db dialect by
    // DBProgramGenerator, lowered and executed.
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
        procedureContext.setStringBuffer(&memory.stringBuffer());

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

    void expectNeighbours(const char* query, const std::vector<NodeID>& expected) {
        NodeColumnSink sink;
        runQuery(query, sink);

        ASSERT_TRUE(sink.otherKinds().empty())
            << "query: " << query << "\nemitted as " << sink.otherKinds().front();
        EXPECT_EQ(sink.values(), expected) << "query: " << query;
    }

    void expectRows(const char* query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    ProcedureManager _procedures;
    std::unique_ptr<Graph> _graph;
};

TEST_F(CallNullableYieldTest, yieldsTheAbsentNodesAsInvalidIDs) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour",
                     {NodeID(0), NodeID(), NodeID(1), NodeID()});
}

TEST_F(CallNullableYieldTest, rendersTheAbsentNodesAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour",
               {{"0"}, {"null"}, {"1"}, {"null"}});
}

// ORDER BY puts nulls last, as it does for a nullable value column
TEST_F(CallNullableYieldTest, sortsTheAbsentNodesLast) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour ORDER BY neighbour",
                     {NodeID(0), NodeID(1), NodeID(), NodeID()});
}

// All nulls key the same, so DISTINCT folds the two absent rows into one
TEST_F(CallNullableYieldTest, dedupsTheAbsentNodesTogether) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN DISTINCT neighbour ORDER BY neighbour",
                     {NodeID(0), NodeID(1), NodeID()});
}

TEST_F(CallNullableYieldTest, testsTheYieldedNodesForNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour IS NULL",
               {{"false"}, {"true"}, {"false"}, {"true"}});
}

// count(x) charges only the rows in which x is not null
TEST_F(CallNullableYieldTest, countsOnlyThePresentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN count(neighbour), count(*)",
               {{"2", "4"}});
}

// The call reads none of the rows in flight, so it is crossed with them: 8 people
// against the call's 4 rows, of which 2 carry a node.
TEST_F(CallNullableYieldTest, crossesTheNullableColumnWithTheRowsInFlight) {
    expectRows("MATCH (n:Person) CALL test.maybeNeighbour() YIELD neighbour "
               "RETURN count(*), count(neighbour)",
               {{"32", "16"}});
}

TEST_F(CallNullableYieldTest, readsTheAbsentNodesPropertyAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour.name",
               {{"Remy"}, {"null"}, {"Adam"}, {"null"}});
}

TEST_F(CallNullableYieldTest, readsTheAbsentNodesPropertyAfterAWith) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour WITH neighbour RETURN neighbour.name",
               {{"Remy"}, {"null"}, {"Adam"}, {"null"}});
}

TEST_F(CallNullableYieldTest, ordersByTheAbsentNodesProperty) {
    RowSink sink;
    runQuery("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour.name ORDER BY neighbour.name", sink);

    const Rows expected {{"Adam"}, {"Remy"}, {"null"}, {"null"}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CallNullableYieldTest, testsTheAbsentNodesLabelAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour:Person",
               {{"true"}, {"null"}, {"true"}, {"null"}});
}

TEST_F(CallNullableYieldTest, projectsTheAbsentNodesAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour {.name}",
               {{"{name: Remy}"}, {"null"}, {"{name: Adam}"}, {"null"}});
}

TEST_F(CallNullableYieldTest, readsTheAbsentNodesKeysAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN keys(neighbour) IS NULL",
               {{"false"}, {"true"}, {"false"}, {"true"}});
}

// Luc (9) stands in for the absent rows
TEST_F(CallNullableYieldTest, coalescesTheAbsentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "WITH neighbour MATCH (luc:Person {name: 'Luc'}) "
               "RETURN coalesce(neighbour, luc)",
               {{"0"}, {"9"}, {"1"}, {"9"}});
}

TEST_F(CallNullableYieldTest, listsTheAbsentNodesAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN [neighbour]",
               {{"[0]"}, {"[null]"}, {"[1]"}, {"[null]"}});
}

// Remy has four out-edges and Adam three
TEST_F(CallNullableYieldTest, comprehendsNoPatternFromTheAbsentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN size([(neighbour)-->(m) | m])",
               {{"4"}, {"0"}, {"3"}, {"0"}});
}

TEST_F(CallNullableYieldTest, matchesNoPatternPredicateFromTheAbsentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "WITH neighbour WHERE (neighbour)-->() RETURN neighbour",
               {{"0"}, {"1"}});
}

TEST_F(CallNullableYieldTest, findsNoExistsPatternFromTheAbsentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "WITH neighbour WHERE EXISTS { (neighbour)-->() } RETURN neighbour",
               {{"0"}, {"1"}});
}

// The two absent rows group together, and an OPTIONAL MATCH from them matches nothing
TEST_F(CallNullableYieldTest, optionallyMatchesNothingFromTheAbsentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "WITH neighbour OPTIONAL MATCH (neighbour)-->(m) RETURN neighbour, count(m)",
               {{"0", "4"}, {"1", "3"}, {"null", "0"}});
}

TEST_F(CallNullableYieldTest, countsOnlyThePresentNodesPerGroup) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "RETURN neighbour IS NULL AS absent, count(neighbour)",
               {{"false", "2"}, {"true", "0"}});
}

TEST_F(CallNullableYieldTest, countsTheDistinctPresentNodesPerGroup) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour "
               "RETURN neighbour IS NULL AS absent, count(DISTINCT neighbour)",
               {{"false", "2"}, {"true", "0"}});
}

TEST_F(CallNullableYieldTest, countsTheDistinctPresentNodes) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN count(DISTINCT neighbour)",
               {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
