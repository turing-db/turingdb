#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
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
#include "columns/ColumnOptVector.h"
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

using OptNodeColumn = ColumnOptVector<NodeID>;

struct NeighbourData : public ProcedureData {};

// test.maybeNeighbour yields four rows as one chunk: node 0, no node, node 1, no node.
// The absent rows are what a plain NODE return value cannot express.
void neighbourExecuteImpl(ProcedureState* procedureState) {
    NeighbourData& data = procedureState->data<NeighbourData>();

    auto* neighbours = static_cast<OptNodeColumn*>(data.getReturnColumn(0));
    neighbours->clear();
    neighbours->push_back(NodeID(0));
    neighbours->push_back(std::nullopt);
    neighbours->push_back(NodeID(1));
    neighbours->push_back(std::nullopt);

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
class OptNodeColumnSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* neighbours = dynamic_cast<const OptNodeColumn*>(chunks[0]);
        if (!neighbours) {
            _otherKinds.emplace_back(chunks[0]->getTypeName());
            return;
        }

        const std::vector<std::optional<NodeID>>& raw = neighbours->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<std::optional<NodeID>>& values() const { return _values; }
    const std::vector<std::string>& otherKinds() const { return _otherKinds; }

private:
    std::vector<std::optional<NodeID>> _values;
    std::vector<std::string> _otherKinds;
};

}

// A procedure declaring a NODE return value nullable writes it through a
// ColumnOptVector<NodeID>, and the ops that carry that column on - the cross product,
// the sort, the dedup, the count - keep it nullable rather than standing an invalid ID
// in for the rows that have no node.
class CallNullableYieldTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* neighbourProcedure = new Procedure("maybeNeighbour");
        neighbourProcedure->setAllocCallback(&neighbourAlloc);
        neighbourProcedure->setDeallocCallback(&neighbourDealloc);
        neighbourProcedure->setExecuteCallback(&neighbourExecute);
        neighbourProcedure->addNullableReturnValue("neighbour", ProcedureType::NODE);
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

    void expectNeighbours(const char* query, const std::vector<std::optional<NodeID>>& expected) {
        OptNodeColumnSink sink;
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

TEST_F(CallNullableYieldTest, yieldsTheNodesAsANullableNodeColumn) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour",
                     {NodeID(0), std::nullopt, NodeID(1), std::nullopt});
}

TEST_F(CallNullableYieldTest, rendersTheAbsentNodesAsNull) {
    expectRows("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour",
               {{"0"}, {"null"}, {"1"}, {"null"}});
}

// ORDER BY puts nulls last, as it does for a nullable value column
TEST_F(CallNullableYieldTest, sortsTheAbsentNodesLast) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour ORDER BY neighbour",
                     {NodeID(0), NodeID(1), std::nullopt, std::nullopt});
}

// All nulls key the same, so DISTINCT folds the two absent rows into one
TEST_F(CallNullableYieldTest, dedupsTheAbsentNodesTogether) {
    expectNeighbours("CALL test.maybeNeighbour() YIELD neighbour RETURN DISTINCT neighbour ORDER BY neighbour",
                     {NodeID(0), NodeID(1), std::nullopt});
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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
