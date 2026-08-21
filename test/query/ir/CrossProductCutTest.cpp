#include <gtest/gtest.h>

#include <stdint.h>

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
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
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

// SimpleGraph's nodes are scanned in ID order, so a cross product of two node scans emits
// outer-major over this sequence: every b for a = Remy, then every b for a = Adam, and so
// on. The first five are enough for every expectation below.
const char* const remy = "Remy";
const char* const adam = "Adam";
const char* const computers = "Computers";
const char* const eighties = "Eighties";

// Remy (0) and Adam (1) are the only nodes carrying an age, and both are 32, so a filter on
// age = 32 keeps exactly those two on whichever side it is written.
const Rows agedPairs = {
    {remy, remy}, {remy, adam}, {adam, remy}, {adam, adam},
};

class NameRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* const column : chunks) {
                const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(column);
                if (!names) {
                    throw TuringException("CrossProductCutTest: expected a string property column");
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

// SKIP and LIMIT over a cross product, through the MLIR frontend: each query is parsed,
// analyzed, generated into the db dialect, then lowered and interpreted, so the assertions
// are the rows a query returns.
//
// A cross product is bounded by the limit budget so the nest can stop early, and that
// bound truncates the product itself. It is only sound while every produced row reaches
// the output: a filter, a skip or a dedup between the two drops rows, and a product cut to
// the budget has then thrown away rows that would have survived. These tests pin the row
// counts on both sides of that line.
class CrossProductCutTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
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

    // The row count alone, for windows over the unfiltered product's 324 rows, where
    // spelling every pair out would say nothing the count does not.
    void expectRowCount(std::string_view query,
                        size_t expected,
                        size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);

        EXPECT_EQ(actual.size(), expected) << "query: " << query;
    }

    // The first `count` pairs of `rows`
    static Rows prefix(const Rows& rows, size_t count) {
        return Rows(rows.begin(), rows.begin() + count);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CrossProductCutTest, emitsEveryFilteredPair) {
    // The baseline the windows below cut into: two aged nodes on each side, four pairs.
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name", agedPairs);
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsToOne) {
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 1",
               prefix(agedPairs, 1));
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsToTwo) {
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 2",
               prefix(agedPairs, 2));
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsPastTheFirstOuterRow) {
    // Three rows reach past the first outer node's pairs, so the nest may not stop once the
    // product has made three rows - the filter dropped some of those.
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 3",
               prefix(agedPairs, 3));
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsToTheirWholeCount) {
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 4",
               agedPairs);
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsPastTheirWholeCount) {
    // A budget wider than the relation keeps every row rather than padding or stopping short.
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 9",
               agedPairs);
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsToNone) {
    const Rows expected = {};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 0",
               expected);
}

TEST_F(CrossProductCutTest, skipsTheFirstFilteredPair) {
    const Rows expected = {{remy, adam}, {adam, remy}, {adam, adam}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 1",
               expected);
}

TEST_F(CrossProductCutTest, skipsPastTheFirstOuterRowOfFilteredPairs) {
    const Rows expected = {{adam, remy}, {adam, adam}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 2",
               expected);
}

TEST_F(CrossProductCutTest, skipsEveryFilteredPair) {
    const Rows expected = {};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 4",
               expected);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfTheFilteredPairs) {
    // The window straddles the first outer node's pairs and the second's, so neither cut may
    // be charged the rows the filter dropped.
    const Rows expected = {{remy, adam}, {adam, remy}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 1 LIMIT 2",
               expected);
}

TEST_F(CrossProductCutTest, cutsAWindowPastTheFirstOuterRowOfFilteredPairs) {
    const Rows expected = {{adam, remy}, {adam, adam}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 2 LIMIT 2",
               expected);
}

TEST_F(CrossProductCutTest, cutsAWindowRunningOffTheEndOfTheFilteredPairs) {
    const Rows expected = {{adam, adam}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 3 LIMIT 2",
               expected);
}

TEST_F(CrossProductCutTest, limitsThePairsOfAFilteredInnerFactor) {
    // The filter is on the inner factor alone: every one of the eighteen outer nodes pairs
    // with the two aged ones, so the product makes far more rows than survive.
    const Rows expected = {{remy, remy}, {remy, adam}, {adam, remy}};
    expectRows("MATCH (a), (b) WHERE b.age = 32 RETURN a.name, b.name LIMIT 3", expected);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfAFilteredInnerFactor) {
    const Rows expected = {{adam, remy}, {adam, adam}};
    expectRows("MATCH (a), (b) WHERE b.age = 32 RETURN a.name, b.name SKIP 2 LIMIT 2", expected);
}

TEST_F(CrossProductCutTest, limitsThePairsOfAFilteredOuterFactor) {
    // The mirror image: filtering the outer factor drops nothing after the product, since
    // the inner loop only ever runs for a surviving outer row.
    const Rows expected = {{remy, remy}, {remy, adam}, {remy, computers}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 RETURN a.name, b.name LIMIT 3", expected);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfAFilteredOuterFactor) {
    const Rows expected = {{remy, adam}, {remy, computers}, {remy, eighties}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 RETURN a.name, b.name SKIP 1 LIMIT 3", expected);
}

TEST_F(CrossProductCutTest, limitsTheUnfilteredPairs) {
    // Nothing drops rows here, so the product may be cut to the budget: the case the bound
    // exists for.
    const Rows expected = {{remy, remy}, {remy, adam}, {remy, computers}};
    expectRows("MATCH (a), (b) RETURN a.name, b.name LIMIT 3", expected);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfTheUnfilteredPairs) {
    // A skip drops rows just as a filter does, so the product may not be cut to the LIMIT
    // alone - the SKIP has to be paid for out of a wider product.
    const Rows expected = {{remy, adam}, {remy, computers}, {remy, eighties}};
    expectRows("MATCH (a), (b) RETURN a.name, b.name SKIP 1 LIMIT 3", expected);
}

TEST_F(CrossProductCutTest, cutsAWideWindowOutOfTheUnfilteredPairs) {
    // 324 pairs in all - eighteen nodes squared - so a window well inside them keeps its
    // full width.
    expectRowCount("MATCH (a), (b) RETURN a.name, b.name SKIP 20 LIMIT 40", 40);
}

TEST_F(CrossProductCutTest, skipsIntoTheSecondOuterRowOfTheUnfilteredPairs) {
    // The skip crosses an outer step (eighteen pairs per outer node), so the budget spans
    // two inner loops.
    expectRowCount("MATCH (a), (b) RETURN a.name, b.name SKIP 17 LIMIT 3", 3);
}

TEST_F(CrossProductCutTest, limitsTheFilteredPairsAcrossEmitChunks) {
    // One row per chunk, so every pair is emitted on its own step and the budget is charged
    // eighteen times over rather than once.
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name LIMIT 3",
               prefix(agedPairs, 3),
               /*chunkSize=*/1);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfTheFilteredPairsAcrossEmitChunks) {
    const Rows expected = {{remy, adam}, {adam, remy}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN a.name, b.name SKIP 1 LIMIT 2",
               expected,
               /*chunkSize=*/1);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfAFilteredInnerFactorAcrossEmitChunks) {
    const Rows expected = {{adam, remy}, {adam, adam}};
    expectRows("MATCH (a), (b) WHERE b.age = 32 RETURN a.name, b.name SKIP 2 LIMIT 2",
               expected,
               /*chunkSize=*/1);
}

TEST_F(CrossProductCutTest, limitsTheDedupedPairs) {
    // A dedup drops rows the same way a filter does: the two aged outer nodes give two
    // distinct names, and a budget of two must reach both.
    const Rows expected = {{remy}, {adam}};
    expectRows("MATCH (a), (b) WHERE a.age = 32 AND b.age = 32 RETURN DISTINCT a.name LIMIT 2",
               expected);
}

TEST_F(CrossProductCutTest, limitsTheThreeWayFilteredPairs) {
    // Three factors, so eight rows: a budget of five reaches into the second outer node's
    // pairs, past two nested products.
    expectRowCount("MATCH (a), (b), (c) WHERE a.age = 32 AND b.age = 32 AND c.age = 32 "
                   "RETURN a.name, b.name, c.name LIMIT 5",
                   5);
}

TEST_F(CrossProductCutTest, cutsAWindowOutOfTheThreeWayFilteredPairs) {
    expectRowCount("MATCH (a), (b), (c) WHERE a.age = 32 AND b.age = 32 AND c.age = 32 "
                   "RETURN a.name, b.name, c.name SKIP 3 LIMIT 4",
                   4);
}
