#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
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
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using SourceCountRow = std::pair<uint64_t, uint64_t>;
using SourceCountRows = std::vector<SourceCountRow>;

// The nine simpledb nodes with an out-edge, by ID, each with its out-degree: the groups
// MATCH (a)-->(b) RETURN a, count(b) reduces the eighteen out-edge rows to. Ordering by
// a is ordering by node ID, so this is also the ascending order the sort must produce.
const SourceCountRows sourceOutDegreesAscending = {
    {0, 4},
    {1, 3},
    {6, 1},
    {8, 2},
    {9, 2},
    {11, 1},
    {12, 2},
    {15, 2},
    {17, 1},
};

// Collects the (node, count) rows in the order the sink sees them. An ORDER BY is only
// correct if that order survives to the output, so nothing here sorts what it collected.
// The count column is the pipeline's one non-nullable value column - a plain
// ColumnVector<uint64_t>, not a ColumnOptVector like every other aggregate's.
class OrderedSourceCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* sources = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(sources, nullptr);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*sources)[rowIndex].getValue(), countRaw[rowIndex]);
        }
    }

    const SourceCountRows& rows() const { return _rows; }

private:
    SourceCountRows _rows;
};

}

// A grouped count is carried through the row-reordering and row-cutting ops beside its
// grouping key. Those ops read a column by kind - an ID chunk or a nullable value chunk -
// and a count is neither: it is the one non-nullable value chunk the pipeline has, so it
// has to be carried on its own uint64 handlers rather than rejected as an unknown kind.
class OrderByCountTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
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
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, sink, &memory);
        interpreter.run();
    }

    void expectRows(std::string_view query, const SourceCountRows& expected) {
        OrderedSourceCountSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    // The rows of @param expected from @param first, as many as @param count - the window
    // a SKIP and a LIMIT cut out of an order
    void windowOf(const SourceCountRows& expected, size_t first, size_t count, SourceCountRows& rows) {
        rows.assign(expected.begin() + first, expected.begin() + first + count);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The count rides along as the sort reorders the groups by their key.
TEST_F(OrderByCountTest, ordersGroupsCarryingTheirCount) {
    expectRows("MATCH (a)-->(b) RETURN a, count(b) ORDER BY a", sourceOutDegreesAscending);
}

// A bounded sort keeps only the best k rows, so the count is also carried by the trim
// that drops the rest, not only by the emit.
TEST_F(OrderByCountTest, ordersGroupsDescendingThenLimits) {
    SourceCountRows expected {sourceOutDegreesAscending.rbegin(), sourceOutDegreesAscending.rbegin() + 3};

    expectRows("MATCH (a)-->(b) RETURN a, count(b) ORDER BY a DESC LIMIT 3", expected);
}

// SKIP and LIMIT together cut a window out of the sorted groups: the skip lifts the
// surviving suffix to the front of a chunk and the limit stops at k, so the count is
// carried by both.
TEST_F(OrderByCountTest, ordersGroupsThenSkipsAndLimits) {
    SourceCountRows expected;
    windowOf(sourceOutDegreesAscending, 2, 3, expected);

    expectRows("MATCH (a)-->(b) RETURN a, count(b) ORDER BY a SKIP 2 LIMIT 3", expected);
}

TEST_F(OrderByCountTest, ordersGroupsThenSkips) {
    SourceCountRows expected;
    windowOf(sourceOutDegreesAscending, 2, sourceOutDegreesAscending.size() - 2, expected);

    expectRows("MATCH (a)-->(b) RETURN a, count(b) ORDER BY a SKIP 2", expected);
}

// A DISTINCT before the sort leaves the groups as they are - one row per group, keyed by
// the grouping key - so the order is the same one, over the same counts.
TEST_F(OrderByCountTest, ordersDistinctGroupsCarryingTheirCount) {
    expectRows("MATCH (a)-->(b) RETURN DISTINCT a, count(b) ORDER BY a", sourceOutDegreesAscending);
}

// Without an ORDER BY the groups come back in the order they were grouped in, which is
// not a guarantee of the language: only the count of rows and their membership are.
TEST_F(OrderByCountTest, skipsAndLimitsGroupsWithoutAnOrder) {
    OrderedSourceCountSink sink;
    runQuery("MATCH (a)-->(b) RETURN a, count(b) SKIP 1 LIMIT 2", &sink);

    const SourceCountRows& rows = sink.rows();
    ASSERT_EQ(rows.size(), 2u);

    for (const SourceCountRow& row : rows) {
        const auto findIt = std::find(sourceOutDegreesAscending.begin(), sourceOutDegreesAscending.end(), row);
        EXPECT_NE(findIt, sourceOutDegreesAscending.end()) << "unexpected row: " << row.first << ", " << row.second;
    }
}

// A count is not orderable by itself: the sort runs before the aggregate that would
// produce the key, so the query is turned away at analysis rather than sorted on a
// column that does not exist yet.
TEST_F(OrderByCountTest, rejectsOrderByOnTheCount) {
    OrderedSourceCountSink sink;
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN a, count(b) ORDER BY count(b)", &sink), TuringException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
