#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

// The eight Person nodes of simpledb, in the order a label scan walks them. Only Remy and
// Adam have a KNOWS_WELL out-edge, so a pattern over it leaves the other six null and the
// rows come out in this order.
const Rows friendRows = {
    {"Remy", "Adam"},
    {"Adam", "Remy"},
    {"Maxime", "null"},
    {"Luc", "null"},
    {"Martina", "null"},
    {"Suhas", "null"},
    {"Cyrus", "null"},
    {"Doruk", "null"},
};

class NameRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* const column : chunks) {
                const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(column);
                if (!names) {
                    throw TuringException("OptionalMatchCutTest: expected a string property column");
                }

                const std::optional<std::string_view>& name = (*names)[rowIndex];
                row.push_back(name ? std::string(*name) : "null");
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

// LIMIT over an OPTIONAL MATCH, through the MLIR frontend: each query is parsed, analyzed,
// generated into the db dialect, then lowered and interpreted.
//
// An optional match accumulates one step of the rows it joins onto rather than the whole
// relation, so - unlike a sort - the loops feeding it can stop as soon as the budget is
// spent. The tests below pin both halves of that: the rows a cut returns, and that the
// budget actually reaches the scan driving the join.
class OptionalMatchCutTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Parses and analyzes a query, generates the db dialect and lowers it into nlModule,
    // whose ops the loop assertions below walk
    void lowerQuery(std::string_view query,
                    mlir::MLIRContext& context,
                    mlir::OwningOpRef<mlir::ModuleOp>& nlModule) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        nlModule = mlir::ModuleOp::create(builder.getUnknownLoc());

        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);
    }

    void runQuery(std::string_view query, Rows& rows, size_t chunkSize) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        NameRowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, chunkSize);
        interpreter.run();

        rows = sink.rows();
    }

    void expectRows(std::string_view query,
                    const Rows& expected,
                    size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    // Whether the loop over an iterator of this kind carries a limit handle. Fails the
    // test when no such loop was lowered, so a query whose shape changed is not read as a
    // silent pass.
    template <typename IteratorOp>
    bool loopOverIsBounded(mlir::ModuleOp nlModule) {
        std::optional<bool> bounded;

        nlModule.walk([&](mlir::nl::For forLoop) {
            if (forLoop.getIterator().getDefiningOp<IteratorOp>()) {
                bounded = forLoop.getLimit() != nullptr;
            }
        });

        EXPECT_TRUE(bounded.has_value()) << "no loop over the expected iterator was lowered";

        return bounded.value_or(false);
    }

    static Rows prefix(const Rows& rows, size_t count) {
        return Rows(rows.begin(), rows.begin() + count);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(OptionalMatchCutTest, emitsEveryRowUncut) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, f.name",
               friendRows);
}

TEST_F(OptionalMatchCutTest, limitsToTheMatchedRows) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name LIMIT 2",
               prefix(friendRows, 2));
}

// Reaching past the matched rows takes padded ones, which are drained after them
TEST_F(OptionalMatchCutTest, limitsPastTheMatchedRowsIntoThePaddedOnes) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name LIMIT 5",
               prefix(friendRows, 5));
}

// A chunk of one puts every row in a step of its own, so the cut lands between steps
// rather than inside one
TEST_F(OptionalMatchCutTest, limitsOneRowPerStep) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name LIMIT 5",
               prefix(friendRows, 5),
               /*chunkSize=*/1);
}

TEST_F(OptionalMatchCutTest, limitsBeyondTheRowsEmitsThemAll) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name LIMIT 20",
               friendRows);
}

// The budget reaches the scan driving the join, not just the loop draining it: the scan
// stops taking steps once the cut is spent rather than walking every remaining node and
// re-running the pattern for each
TEST_F(OptionalMatchCutTest, boundsTheScanDrivingTheJoin) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    mlir::OwningOpRef<mlir::ModuleOp> nlModule;
    lowerQuery("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name LIMIT 3",
               context,
               nlModule);

    EXPECT_TRUE(loopOverIsBounded<mlir::nl::ScanNodesByLabel>(*nlModule));
    EXPECT_TRUE(loopOverIsBounded<mlir::nl::OptionalDrain>(*nlModule));
}

// A sort between the cut and the join is a pipeline breaker of the whole relation, so the
// scan must still see every row: the budget stops at the sort's emit loop
TEST_F(OptionalMatchCutTest, leavesTheScanUnboundedUnderASort) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    mlir::OwningOpRef<mlir::ModuleOp> nlModule;
    lowerQuery("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, f.name ORDER BY p.name LIMIT 3",
               context,
               nlModule);

    EXPECT_FALSE(loopOverIsBounded<mlir::nl::ScanNodesByLabel>(*nlModule));
}
