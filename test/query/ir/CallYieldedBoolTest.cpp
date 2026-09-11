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
#include "columns/ColumnVector.h"
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

#include "IRTestRows.h"
#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using Flags = std::vector<bool>;
using BoolColumn = ColumnVector<types::Bool::Primitive>;

struct FlagsData : public ProcedureData {};

// test.flags yields the four flags true, false, false, true as one chunk
void flagsExecuteImpl(ProcedureState* procedureState) {
    FlagsData& data = procedureState->data<FlagsData>();

    auto* flags = static_cast<BoolColumn*>(data.getReturnColumn(0));
    flags->clear();
    flags->push_back(true);
    flags->push_back(false);
    flags->push_back(false);
    flags->push_back(true);

    procedureState->finish();
}

void flagsExecute(ProcedureState* procedureState) {
    switch (procedureState->getStep()) {
        case ProcedureState::Step::PREPARE:
        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
        flagsExecuteImpl(procedureState);
        break;
    }
}

ProcedureData* flagsAlloc() {
    return new FlagsData();
}

void flagsDealloc(ProcedureData* data) {
    delete data;
}

// Collects the one boolean value column a query emits, and the type name of any chunk
// that reached the sink as something else
class BoolColumnSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* flags = dynamic_cast<const BoolColumn*>(chunks[0]);
        if (!flags) {
            _otherKinds.emplace_back(chunks[0]->getTypeName());
            return;
        }

        const std::vector<types::Bool::Primitive>& raw = flags->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(static_cast<bool>(raw[rowIndex]));
        }
    }

    const Flags& values() const { return _values; }
    const std::vector<std::string>& otherKinds() const { return _otherKinds; }

private:
    Flags _values;
    std::vector<std::string> _otherKinds;
};

}

// A procedure writes a BOOLEAN return value into a plain boolean column, the column a
// comparison between values also produces, and the ops that carry it on - the sort, the
// dedup, the collect - keep that column: it is a value, not a mask.
class CallYieldedBoolTest : public TuringTest {
protected:
    void initialize() override {
        _procedures.init();

        ProcedureNamespace* testNamespace = _procedures.createNamespace("test");

        Procedure* flagsProcedure = new Procedure("flags");
        flagsProcedure->setAllocCallback(&flagsAlloc);
        flagsProcedure->setDeallocCallback(&flagsDealloc);
        flagsProcedure->setExecuteCallback(&flagsExecute);
        flagsProcedure->addReturnValue("flag", ProcedureType::BOOL);
        testNamespace->addProcedure(flagsProcedure);

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
        analyzer.setV3();
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

    void expectFlags(const char* query, const Flags& expected) {
        BoolColumnSink sink;
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

TEST_F(CallYieldedBoolTest, yieldsTheFlagsAsABooleanColumn) {
    expectFlags("CALL test.flags() YIELD flag RETURN flag", {true, false, false, true});
}

TEST_F(CallYieldedBoolTest, sortsTheYieldedFlags) {
    expectFlags("CALL test.flags() YIELD flag RETURN flag ORDER BY flag", {false, false, true, true});
}

TEST_F(CallYieldedBoolTest, keepsTheFirstSortedFlags) {
    expectFlags("CALL test.flags() YIELD flag RETURN flag ORDER BY flag DESC LIMIT 2", {true, true});
}

TEST_F(CallYieldedBoolTest, dedupsTheYieldedFlags) {
    expectFlags("CALL test.flags() YIELD flag RETURN DISTINCT flag ORDER BY flag", {false, true});
}

TEST_F(CallYieldedBoolTest, collectsTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag RETURN collect(flag)", {{"[true, false, false, true]"}});
}

TEST_F(CallYieldedBoolTest, filtersOnTheYieldedFlags) {
    expectRows("CALL test.flags() YIELD flag WITH flag WHERE flag RETURN count(*)", {{"2"}});
}

TEST_F(CallYieldedBoolTest, testsTheYieldedFlagsForNull) {
    expectRows("CALL test.flags() YIELD flag RETURN flag IS NULL",
               {{"false"}, {"false"}, {"false"}, {"false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
