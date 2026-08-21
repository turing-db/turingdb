#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
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
using ConstantNameRow = std::pair<int64_t, std::string>;
using ConstantNameRows = std::vector<ConstantNameRow>;
using Int64Values = std::vector<int64_t>;
using StringValues = std::vector<std::string>;
using ConstantPair = std::pair<int64_t, int64_t>;
using ConstantPairs = std::vector<ConstantPair>;
using OptInt64Values = std::vector<std::optional<int64_t>>;
using NodeValueRow = std::pair<uint64_t, std::optional<int64_t>>;
using NodeValueRows = std::vector<NodeValueRow>;
using NodeAverageRow = std::pair<uint64_t, std::optional<double>>;
using NodeAverageRows = std::vector<NodeAverageRow>;
using NodeCountRow = std::pair<uint64_t, uint64_t>;
using NodeCountRows = std::vector<NodeCountRow>;
using Counts = std::vector<uint64_t>;

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

// Collects a projection that pairs a constant with a name. The constant column is a
// ColumnConst, which holds one value for every row of the chunk rather than one per row,
// so it is read through the same subscript at each of them.
class DistinctConstantNameSink : public NLOutputSink {
public:
    DistinctConstantNameSink(size_t constantColumn, size_t nameColumn)
        : _constantColumn(constantColumn),
          _nameColumn(nameColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks[_constantColumn]);
        ASSERT_NE(constants, nullptr);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[_nameColumn]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back((*constants)[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    void sortedRows(ConstantNameRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    ConstantNameRows _rows;
    size_t _constantColumn {0};
    size_t _nameColumn {0};
};

class DistinctNodeValueSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(values, nullptr);

        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(readNodeID(chunks[0], rowIndex), valueRaw[rowIndex]);
        }
    }

    void sortedRows(NodeValueRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NodeValueRows _rows;
};

// avg widens to f64 whatever it reduces, so the aggregate column is nullable double.
class DistinctNodeAverageSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* averages = dynamic_cast<const ColumnOptVector<double>*>(chunks[1]);
        ASSERT_NE(averages, nullptr);

        const auto& averageRaw = averages->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(readNodeID(chunks[0], rowIndex), averageRaw[rowIndex]);
        }
    }

    void sortedRows(NodeAverageRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NodeAverageRows _rows;
};

// A count is never null and is not an ID, so its column is the pipeline's one
// non-nullable value column: a plain ColumnVector<uint64_t> rather than a
// ColumnOptVector like every other aggregate's.
class DistinctNodeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(readNodeID(chunks[0], rowIndex), countRaw[rowIndex]);
        }
    }

    void sortedRows(NodeCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NodeCountRows _rows;
};

// The one-column sibling of DistinctNodeCountSink: a count with nothing to group by.
class DistinctCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _counts.push_back(countRaw[rowIndex]);
        }
    }

    const Counts& counts() const { return _counts; }

private:
    Counts _counts;
};

// Collects a projection of one constant column, which holds a single value for however
// many rows the chunk carries rather than one per row. A constant a cut is charged to is
// laid out over the rows of the driving relation, so the matched cases read their column
// through DistinctOptInt64Sink instead.
class DistinctConstantSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks[0]);
        ASSERT_NE(constants, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back((*constants)[rowIndex]);
        }
    }

    const Int64Values& values() const { return _values; }

private:
    Int64Values _values;
};

// The two-column sibling of DistinctConstantSink: a projection of two constants, whose
// rows are told apart by their count alone.
class DistinctConstantPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* firsts = dynamic_cast<const ColumnConst<int64_t>*>(chunks[0]);
        ASSERT_NE(firsts, nullptr);

        const auto* seconds = dynamic_cast<const ColumnConst<int64_t>*>(chunks[1]);
        ASSERT_NE(seconds, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*firsts)[rowIndex], (*seconds)[rowIndex]);
        }
    }

    const ConstantPairs& rows() const { return _rows; }

private:
    ConstantPairs _rows;
};

// The string sibling of DistinctConstantSink: a string literal is one string_view standing
// for every row, where a name read off a node comes as a column of optional views.
class DistinctConstantStringSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* constants = dynamic_cast<const ColumnConst<std::string_view>*>(chunks[0]);
        ASSERT_NE(constants, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.emplace_back((*constants)[rowIndex]);
        }
    }

    const StringValues& values() const { return _values; }

private:
    StringValues _values;
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
        analyzer.setV3();
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

    // One row per name, each carrying the same constant: the rows a projection of a
    // constant beside a name emits once the dedup has run
    void constantNameRowsFor(int64_t constant, const Names& names, ConstantNameRows& rows) {
        rows.clear();
        for (const std::string& name : names) {
            rows.emplace_back(constant, name);
        }
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

// The addition is a function of a, so it tells two rows apart exactly when a already
// does: the 18 out-edge rows carry the same 9 sources returning a alone gives.
TEST_F(DistinctTest, dedupsANodeWithAnExpressionOverIt) {
    DistinctNodeValueSink sink;
    runQuery("MATCH (a)-->(b) RETURN DISTINCT a, a.age + 42", &sink);

    const NodeValueRows expected = {
        {0, 74},
        {1, 74},
        {6, std::nullopt},
        {8, std::nullopt},
        {9, std::nullopt},
        {11, std::nullopt},
        {12, std::nullopt},
        {15, std::nullopt},
        {17, std::nullopt},
    };

    NodeValueRows actual;
    sink.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

// A constant column holds the same value in every row, so it tells no two rows apart:
// the 18 out-edge rows carry the same 12 distinct names they do without it, each still
// paired with the constant. It is also read outside the loop the names are read in, so
// deduping on it would place the dedup above their definition.
TEST_F(DistinctTest, dedupsPastALeadingConstant) {
    DistinctConstantNameSink sink(0, 1);
    runQuery("MATCH (a)-->(b) RETURN DISTINCT 5, b.name", &sink);

    ConstantNameRows expected;
    constantNameRowsFor(5, distinctTargetNames, expected);

    ConstantNameRows actual;
    sink.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

// The same projection the other way round: which column the constant is does not change
// what the rows are, so this is the same 12 rows.
TEST_F(DistinctTest, dedupsPastATrailingConstant) {
    DistinctConstantNameSink sink(1, 0);
    runQuery("MATCH (a)-->(b) RETURN DISTINCT b.name, 5", &sink);

    ConstantNameRows expected;
    constantNameRowsFor(5, distinctTargetNames, expected);

    ConstantNameRows actual;
    sink.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

// Every column of the projection is constant, so its 18 rows are one row repeated and
// no column tells two of them apart: what the dedup keeps is the first row alone, which
// codegen charges to the constants as a cap of one row rather than to a dedup keyed on a
// column that never varies.
TEST_F(DistinctTest, keepsOneRowOfAMatchedConstantProjection) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (n) RETURN DISTINCT 5", &sink);

    const OptInt64Values expected = {5};
    EXPECT_EQ(sink.values(), expected);
}

// The same projection with nothing matched, which is one row to begin with: the cap has
// no repeat to cut and the row comes back as it is.
TEST_F(DistinctTest, keepsTheOneRowOfAConstantProjection) {
    DistinctConstantSink sink;
    runQuery("RETURN DISTINCT 42", &sink);

    const Int64Values expected = {42};
    EXPECT_EQ(sink.values(), expected);
}

// An expression over constants alone is constant too, so it is capped the same way
// rather than deduped.
TEST_F(DistinctTest, keepsTheOneRowOfAConstantExpression) {
    DistinctConstantSink sink;
    runQuery("RETURN DISTINCT 40 + 2", &sink);

    const Int64Values expected = {42};
    EXPECT_EQ(sink.values(), expected);
}

// Every column of the projection is capped, not just the first: two constants are one row
// of two columns, and the row that survives carries both.
TEST_F(DistinctTest, keepsOneRowOfEveryConstantColumn) {
    DistinctConstantPairSink sink;
    runQuery("RETURN DISTINCT 5, 7", &sink);

    const ConstantPairs expected = {{5, 7}};
    EXPECT_EQ(sink.rows(), expected);
}

// The cross product emits its 44 rows over several steps of a nest, where the cap is
// charged step by step: the row survives the first of them and every later step is left
// with none, so the repeats do not come back one per step.
TEST_F(DistinctTest, keepsOneRowOfAConstantProjectionAcrossANest) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (a)-->(b), (a)-->(c) RETURN DISTINCT 5", &sink);

    const OptInt64Values expected = {5};
    EXPECT_EQ(sink.values(), expected);
}

// A LIMIT after the dedup is charged to the row the dedup left, not to the 18 the match
// made, so a limit of more than one row keeps that single row.
TEST_F(DistinctTest, limitsTheOneRowOfAConstantProjection) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (n) RETURN DISTINCT 5 LIMIT 3", &sink);

    const OptInt64Values expected = {5};
    EXPECT_EQ(sink.values(), expected);
}

// The SKIP sibling: the deduped projection is one row, so skipping a row leaves nothing.
TEST_F(DistinctTest, skipsTheOneRowOfAConstantProjection) {
    DistinctOptInt64Sink sink;
    runQuery("MATCH (n) RETURN DISTINCT 5 SKIP 1", &sink);

    EXPECT_TRUE(sink.values().empty());
}

// A SKIP and a LIMIT after the dedup are two cuts chained over its cap, so the LIMIT reads
// the constant column the SKIP emitted rather than the one the projection computed.
TEST_F(DistinctTest, keepsTheOneRowOfAConstantProjectionUnderAWindow) {
    DistinctConstantSink sink;
    runQuery("RETURN DISTINCT 42 SKIP 0 LIMIT 1", &sink);

    const Int64Values expected = {42};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DistinctTest, keepsTheOneRowOfAConstantExpressionUnderAWindow) {
    DistinctConstantSink sink;
    runQuery("RETURN DISTINCT 40 + 2 SKIP 0 LIMIT 1", &sink);

    const Int64Values expected = {42};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DistinctTest, keepsTheOneRowOfAConstantStringProjectionUnderAWindow) {
    DistinctConstantStringSink sink;
    runQuery("RETURN DISTINCT 'abc' SKIP 0 LIMIT 1", &sink);

    const StringValues expected = {"abc"};
    EXPECT_EQ(sink.values(), expected);
}

// The projection is a grouped aggregate first, so the dedup runs on its results, where a
// group key is unique by construction: DISTINCT can never drop a row here.
TEST_F(DistinctTest, keepsEveryGroupOfAnAggregate) {
    DistinctNodeAverageSink sink;
    runQuery("MATCH (n) RETURN DISTINCT n, avg(n.age)", &sink);

    const NodeAverageRows expected = {
        {0, 32.0},
        {1, 32.0},
        {2, std::nullopt},
        {3, std::nullopt},
        {4, std::nullopt},
        {5, std::nullopt},
        {6, std::nullopt},
        {7, std::nullopt},
        {8, std::nullopt},
        {9, std::nullopt},
        {10, std::nullopt},
        {11, std::nullopt},
        {12, std::nullopt},
        {13, std::nullopt},
        {14, std::nullopt},
        {15, std::nullopt},
        {16, std::nullopt},
        {17, std::nullopt},
    };

    NodeAverageRows actual;
    sink.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

// The count sibling of keepsEveryGroupOfAnAggregate: the 18 out-edge rows group into the
// 9 sources that have one, each carrying its out-degree. A group is its key, so no two of
// those rows can be equal and there is nothing to dedup - which is what lets a count
// through, a column the dedup has no way to key a row by.
TEST_F(DistinctTest, keepsEveryGroupOfACount) {
    DistinctNodeCountSink sink;
    runQuery("MATCH (a)-->(b) RETURN DISTINCT a, count(b)", &sink);

    const NodeCountRows expected = {
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

    NodeCountRows actual;
    sink.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

// A count with nothing to group by is one row, so there is no second row a dedup could
// find equal to it: the 18 nodes are counted once and the count comes back as it is.
TEST_F(DistinctTest, keepsAScalarCount) {
    DistinctCountSink sink;
    runQuery("MATCH (n) RETURN DISTINCT count(n)", &sink);

    const Counts expected = {18};
    EXPECT_EQ(sink.counts(), expected);
}

// DISTINCT over a projected property, ordered by that same property: the ORDER BY key
// is a column the projection carries, so it sorts the deduped names in place.
TEST_F(DistinctTest, dedupsAndOrdersProjectedNames) {
    expectNames("MATCH (a)-->(b) RETURN DISTINCT b.name ORDER BY b.name", distinctTargetNames);
}

// The same query with the returned property aliased, and the alias as the sort key. The
// key is one variable the projection declares, not a second reading of b.name, so it is
// matched to its item by that declaration rather than by structure - and the dedup then
// sorts on a column it carries, exactly as it does without the alias.
TEST_F(DistinctTest, dedupsAndOrdersAnAliasedProperty) {
    expectNames("MATCH (a)-->(b) RETURN DISTINCT b.name AS targetName ORDER BY targetName",
                distinctTargetNames);
}

// DISTINCT then ORDER BY: the sort sees the deduped rows, so the 12 distinct targets
// come back once each, ascending.
TEST_F(DistinctTest, ordersDistinctTargets) {
    const Rows expected = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {10}, {13}, {14}, {16}};
    expectNodeRows("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b", expected);
}

// A wildcard is what declares the projection's columns, so a key naming one of them is
// only a returned column once the expansion has run. The scan emits each node once, so
// the dedup drops nothing and all 18 come back in the order the key asks for.
TEST_F(DistinctTest, ordersDistinctWildcardColumn) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectNodeRows("MATCH (a) RETURN DISTINCT * ORDER BY a", expected);
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
// rejects the query for the same reason, and so does the analyzer.
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

// The generated program holds no dedup at all for a projection of constants: the rows
// are one row repeated, so a db.remove_duplicates would be keyed on columns that never
// vary. It is a db.limit of one row instead, which is what keeps the first row.
TEST_F(DistinctTest, capsAConstantProjectionInsteadOfDedupingIt) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN DISTINCT 5", context, module);

    mlir::db::Limit limitOp;
    size_t distinctCount = 0;
    size_t limitCount = 0;

    module->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::db::RemoveDuplicates>(operation)) {
            distinctCount++;
        } else if (mlir::db::Limit found = mlir::dyn_cast<mlir::db::Limit>(operation)) {
            limitOp = found;
            limitCount++;
        }
    });

    EXPECT_EQ(distinctCount, 0u);
    ASSERT_EQ(limitCount, 1u);
    EXPECT_EQ(limitOp.getCount(), 1u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
