#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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
#include "DBOps.h"
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
#include "columns/ColumnOptVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<uint64_t>;
using Rows = std::vector<Row>;
using Names = std::vector<std::string>;
using OptInt64Values = std::vector<std::optional<int64_t>>;

// Every distinct out-edge target of simpledb by name. Eighteen edges point at these
// twelve nodes - Gym is pointed at three times, Remy, Computers, Bio and Cooking twice
// each - so this is the row set DISTINCT must cut them down to.
const Names distinctTargetNames = {
    "Adam", "Animals", "Bio", "Computers", "Cooking", "Eighties",
    "Ghosts", "Gym", "JiuJitsu", "Padel", "Remy", "Travel",
};

uint64_t readNodeID(const Column* column, size_t row) {
    const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column);
    if (!nodeIDs) {
        throw std::runtime_error("DistinctTest: expected a node ID output column");
    }

    return (*nodeIDs)[row].getValue();
}

void describeRows(const Rows& rows, std::string& out) {
    out.clear();
    for (const Row& row : rows) {
        out += "        {";
        for (size_t index = 0; index < row.size(); index++) {
            if (index > 0) {
                out += ", ";
            }

            out += std::to_string(row[index]);
        }

        out += "},\n";
    }
}

// Collects the node ID rows a projection emits, in the order the sink sees them. A plain
// DISTINCT emits its survivors in scan order, which is not a guarantee of the language,
// so only the tests that also ORDER BY read rows(); the others read sortedRows().
class DistinctNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* const column : chunks) {
                row.push_back(readNodeID(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

    void sortedRows(Rows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    Rows _rows;
};

// The string sibling of DistinctNodeSink: collects a single projected string property
// column, in sink order.
class DistinctNameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _names.push_back(std::string(*nameRaw[rowIndex]));
        }
    }

    const Names& names() const { return _names; }

private:
    Names _names;
};

// Collects a single projected nullable integer property column. Most simpledb nodes
// carry no age at all, so this is the sink that sees what DISTINCT does with nulls.
class DistinctOptInt64Sink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(valueRaw[rowIndex]);
        }
    }

    const OptInt64Values& values() const { return _values; }

    void sortedValues(OptInt64Values& values) const {
        values = _values;
        std::sort(values.begin(), values.end());
    }

private:
    OptInt64Values _values;
};

}

// End-to-end DISTINCT through the MLIR frontend: each query is parsed, analyzed,
// generated into the db dialect by DBProgramGenerator, then lowered and interpreted.
// The assertions are on the emitted row set, which is what the generated
// db.remove_duplicates decides.
class DistinctTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Generates the db dialect program of a query into @param module, without running it
    void generateProgram(std::string_view query,
                         mlir::MLIRContext& context,
                         mlir::OwningOpRef<mlir::ModuleOp>& module) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory);
        interpreter.run();
    }

    // The row set a DISTINCT emits, compared order-independently: a dedup without an
    // ORDER BY promises the rows, not their order
    void expectNodeRowSet(std::string_view query, const Rows& expected) {
        DistinctNodeSink sink;
        runQuery(query, &sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string description;
        describeRows(actual, description);

        EXPECT_EQ(actual, sortedExpected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    // The rows a DISTINCT ... ORDER BY emits, in the order the sink saw them: here the
    // order is part of the result, so nothing sorts it
    void expectNodeRows(std::string_view query, const Rows& expected) {
        DistinctNodeSink sink;
        runQuery(query, &sink);

        std::string description;
        describeRows(sink.rows(), description);

        EXPECT_EQ(sink.rows(), expected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    void expectNames(std::string_view query, const Names& expected) {
        DistinctNameSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.names(), expected) << "query: " << query;
    }

    // One single-column row per name, holding the ID of the node with that name, so a
    // set of nodes can be spelled out by name
    void nodeRowsFor(const Names& names, Rows& rows) {
        rows.clear();
        for (const std::string& name : names) {
            rows.push_back({SimpleGraph::findNodeID(_graph, name).getValue()});
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// Several sources point at the same target, so the 18 out-edge rows carry only 12
// distinct targets - the row set DISTINCT keeps.
TEST_F(DistinctTest, dedupsRepeatedTargets) {
    Rows expected;
    nodeRowsFor(distinctTargetNames, expected);

    expectNodeRowSet("MATCH (a)-->(b) RETURN DISTINCT b", expected);
}

// The mirror case: a source repeats once per out-edge, so the same 18 rows carry only
// the 9 nodes that have any out-edge.
TEST_F(DistinctTest, dedupsRepeatedSources) {
    const Rows expected = {{0}, {1}, {6}, {8}, {9}, {11}, {12}, {15}, {17}};
    expectNodeRowSet("MATCH (a)-->(b) RETURN DISTINCT a", expected);
}

// A DISTINCT over a cross product: the two hops multiply, so a source repeats once per
// pair of out-edges - 44 rows for the same 9 nodes. The dedup filters in the innermost
// loop of the nest, so the repetition the cross product creates is dropped too.
TEST_F(DistinctTest, dedupsAcrossACrossProduct) {
    const Rows expected = {{0}, {1}, {6}, {8}, {9}, {11}, {12}, {15}, {17}};
    expectNodeRowSet("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT a", expected);
}

// The same cross product, projecting both columns of its first arm: every (a, b) pair is
// already distinct - one per out-edge - and only the unreturned second arm repeats it,
// once per out-edge of a. So the 44 rows carry all 18 pairs and DISTINCT drops nothing
// but that repetition, where dedupsAcrossACrossProduct cut the same 44 rows down to 9.
TEST_F(DistinctTest, dedupsCrossProductRepeatsOfAProjectedPair) {
    const Rows expected = {
        {0, 1}, {0, 2}, {0, 3}, {0, 6},
        {1, 0}, {1, 4}, {1, 5},
        {6, 0},
        {8, 4}, {8, 7},
        {9, 2}, {9, 10},
        {11, 5},
        {12, 13}, {12, 16},
        {15, 13}, {15, 14},
        {17, 13},
    };
    expectNodeRowSet("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT a, b", expected);
}

// A row's identity is the whole projection, not one column: the two-hop pairs hold
// (0, 0) twice - Remy reaches himself through Adam and through Ghosts - and that is the
// only pair DISTINCT drops. Each column keeps its own repeats, so a is still 0 three
// times in the surviving rows.
TEST_F(DistinctTest, dedupsRowsAcrossColumns) {
    const Rows expected = {
        {0, 0}, {0, 4}, {0, 5},
        {1, 1}, {1, 2}, {1, 3}, {1, 6},
        {6, 1}, {6, 2}, {6, 3}, {6, 6},
    };
    expectNodeRowSet("MATCH (a)-->(b)-->(c) RETURN DISTINCT a, c", expected);
}

// A scan emits each node once, so DISTINCT has nothing to drop: all 18 nodes come back.
TEST_F(DistinctTest, keepsAlreadyDistinctRows) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectNodeRowSet("MATCH (n) RETURN DISTINCT n", expected);
}

// Only Remy and Adam have an age, both 32, and every other node's age is null. The two
// 32s collapse into one row and - as in Cypher - so do the sixteen nulls.
TEST_F(DistinctTest, dedupsNullsTogether) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (n) RETURN DISTINCT n.age", &sink);

    const OptInt64Values expected = {std::nullopt, 32};
    OptInt64Values actual;
    sink.sortedValues(actual);

    EXPECT_EQ(actual, expected);
}

// DISTINCT over a projected property, ordered by that same property: the ORDER BY key
// is a column the projection carries, so it sorts the deduped names in place.
TEST_F(DistinctTest, dedupsAndOrdersProjectedNames) {
    expectNames("MATCH (a)-->(b) RETURN DISTINCT b.name ORDER BY b.name", distinctTargetNames);
}

// DISTINCT then ORDER BY: the sort sees the deduped rows, so the 12 distinct targets
// come back once each, ascending.
TEST_F(DistinctTest, ordersDistinctTargets) {
    const Rows expected = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {10}, {13}, {14}, {16}};
    expectNodeRows("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b", expected);
}

// ORDER BY ... LIMIT k over a DISTINCT: the k best rows of the distinct order, so the
// three highest distinct target IDs.
TEST_F(DistinctTest, ordersDistinctTargetsThenLimits) {
    const Rows expected = {{16}, {14}, {13}};
    expectNodeRows("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b DESC LIMIT 3", expected);
}

// SKIP m drops the first m rows of the distinct order, not of the scan: 12 distinct
// targets ascending, minus the first 10.
TEST_F(DistinctTest, ordersDistinctTargetsThenSkips) {
    const Rows expected = {{14}, {16}};
    expectNodeRows("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b SKIP 10", expected);
}

// LIMIT with no ORDER BY charges the distinct rows, not the scanned ones: it emits k
// rows, and they are still k different rows. Which ones is the scan order, so only the
// count and the distinctness are asserted.
TEST_F(DistinctTest, limitsDistinctRows) {
    DistinctNodeSink sink;
    runQuery("MATCH (a)-->(b) RETURN DISTINCT b LIMIT 5", &sink);

    Rows rows;
    sink.sortedRows(rows);

    ASSERT_EQ(rows.size(), 5u);
    EXPECT_EQ(std::unique(rows.begin(), rows.end()), rows.end());
}

// A key the projection does not carry cannot be honoured after the dedup: it was read
// once per pre-dedup row, so it no longer lines up with the rows that survived. Cypher
// rejects the query for the same reason, and so does the generator.
TEST_F(DistinctTest, rejectsOrderByOnUnreturnedKey) {
    DistinctNodeSink sink;
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b.name", &sink),
                 TuringException);
}

// A DISTINCT projection ordered by the very expression it returns. The key and the
// returned item are two separate trees, so recognising them as one column is what keeps
// this query from being turned away - after the dedup, only a column the projection
// carries can be sorted on. The difference is 0 for the four pairs of aged Person nodes
// and null for the other sixty, so the dedup leaves two rows and the null sorts last.
TEST_F(DistinctTest, dedupsAndOrdersAnExpression) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (a:Person), (b:Person) RETURN DISTINCT b.age - a.age ORDER BY b.age - a.age",
             &sink);

    const OptInt64Values expected = {0, std::nullopt};
    EXPECT_EQ(sink.values(), expected);
}

// DISTINCT then ORDER BY over a cross product: the sort sees the 9 rows the dedup left,
// not the 44 the product built, so the order is over distinct sources. Sorting first
// would order 44 rows and dedup a sorted stream - the same row set, but the sort would
// carry five times the rows.
TEST_F(DistinctTest, ordersDistinctSourcesAcrossACrossProduct) {
    const Rows expected = {{0}, {1}, {6}, {8}, {9}, {11}, {12}, {15}, {17}};
    expectNodeRows("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT a ORDER BY a", expected);
}

// Both columns of the product's first arm, deduped and then ordered on both: the sort
// keys are the two columns the projection carries, so the 18 surviving pairs come back
// in lexicographic order - b ascending within each a.
TEST_F(DistinctTest, ordersDistinctCrossProductPairs) {
    const Rows expected = {
        {0, 1}, {0, 2}, {0, 3}, {0, 6},
        {1, 0}, {1, 4}, {1, 5},
        {6, 0},
        {8, 4}, {8, 7},
        {9, 2}, {9, 10},
        {11, 5},
        {12, 13}, {12, 16},
        {15, 13}, {15, 14},
        {17, 13},
    };
    expectNodeRows("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT a, b ORDER BY a, b", expected);
}

// Top-k over a deduped cross product: the limit is charged to the distinct rows, so this
// is the three highest sources. Charged to the product's rows instead it would return
// 17, 15, 15 - node 15 has two out-edges, so the product repeats it four times.
TEST_F(DistinctTest, ordersDistinctSourcesThenLimitsAcrossACrossProduct) {
    const Rows expected = {{17}, {15}, {12}};
    expectNodeRows("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT a ORDER BY a DESC LIMIT 3", expected);
}

// The generated program dedups before it sorts - the sort's column is the dedup's
// result - which is what makes the sort order the distinct rows rather than the raw
// ones. The reverse order would sort every duplicate and dedup the sorted rows.
TEST_F(DistinctTest, dedupsBeforeSorting) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b", context, module);

    mlir::db::RemoveDuplicates distinctOp;
    mlir::db::Sort sortOp;
    size_t distinctCount = 0;

    module->walk([&](mlir::Operation* operation) {
        if (mlir::db::RemoveDuplicates found = mlir::dyn_cast<mlir::db::RemoveDuplicates>(operation)) {
            distinctOp = found;
            distinctCount++;
        } else if (mlir::db::Sort found = mlir::dyn_cast<mlir::db::Sort>(operation)) {
            sortOp = found;
        }
    });

    ASSERT_TRUE(distinctOp);
    ASSERT_TRUE(sortOp);
    EXPECT_EQ(distinctCount, 1u);

    // One deduped column in, one sorted column out, and the sort reads the dedup
    ASSERT_EQ(distinctOp.getResults().size(), 1u);
    ASSERT_EQ(sortOp.getColumns().size(), 1u);
    EXPECT_EQ(sortOp.getColumns().front().getDefiningOp(), distinctOp.getOperation());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
