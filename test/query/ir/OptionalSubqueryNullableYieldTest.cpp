#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>

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
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
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

// test.maybeNeighbour yields four rows as one chunk: node 0, no node, node 1, no node
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

}

// An OPTIONAL CALL whose body hands back a nullable ID column: the drain re-chunks it as
// the ColumnOptVector it is, and pads the input rows the body yielded nothing for
class OptionalSubqueryNullableYieldTest : public TuringTest {
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

// Remy is one input row and the body yields its four for it
TEST_F(OptionalSubqueryNullableYieldTest, handsTheNullableNodesBackThroughTheDrain) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "OPTIONAL CALL { CALL test.maybeNeighbour() YIELD neighbour RETURN neighbour } "
               "RETURN p.name, neighbour",
               {{"Remy", "0"}, {"Remy", "null"}, {"Remy", "1"}, {"Remy", "null"}});
}

// Only Remy and Adam are 32, so the body yields nothing for the six other Persons and each
// of them comes back once with no node
TEST_F(OptionalSubqueryNullableYieldTest, padsTheInputRowsTheBodyYieldedNoNodeFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { CALL test.maybeNeighbour() YIELD neighbour WHERE p.age = 32 RETURN neighbour } "
               "RETURN p.name, neighbour",
               {{"Remy", "0"}, {"Remy", "null"}, {"Remy", "1"}, {"Remy", "null"},
                {"Adam", "0"}, {"Adam", "null"}, {"Adam", "1"}, {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}
