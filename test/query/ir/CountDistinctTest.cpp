#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Counts = std::vector<uint64_t>;
using CountPair = std::pair<uint64_t, uint64_t>;
using CountPairs = std::vector<CountPair>;
using NameCountRow = std::pair<std::optional<std::string>, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;
using AgeCountRow = std::pair<std::optional<int64_t>, uint64_t>;
using AgeCountRows = std::vector<AgeCountRow>;
using NameCountCountRow = std::tuple<std::optional<std::string>, uint64_t, uint64_t>;
using NameCountCountRows = std::vector<NameCountCountRow>;
using NameSumCountRow = std::tuple<std::optional<std::string>, std::optional<int64_t>, uint64_t>;
using NameSumCountRows = std::vector<NameSumCountRow>;

// Collects the single-column ui64 rows a scalar count emits. A count collapses to one
// row, so a test reads values().front() - but the vector keeps whatever came out, so a
// count that wrongly emitted more than one row fails loudly.
class ScalarCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& raw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const Counts& values() const { return _values; }

private:
    Counts _values;
};

// Collects the two-column ui64 rows a projection of two scalar counts emits, so a test
// can show that two counts in one projection keep their own tallies.
class ScalarCountPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* firsts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        const auto* seconds = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(firsts, nullptr);
        ASSERT_NE(seconds, nullptr);

        const auto& firstRaw = firsts->getRaw();
        const auto& secondRaw = seconds->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(firstRaw[rowIndex], secondRaw[rowIndex]);
        }
    }

    const CountPairs& rows() const { return _rows; }

private:
    CountPairs _rows;
};

// Collects the (name, count) rows a grouped count emits: a nullable string key chunk
// and a non-null ui64 count chunk. A grouped aggregate emits its groups in first-seen
// order, which the language does not promise, so the tests compare sorted row sets.
class GroupedCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, countRaw[rowIndex]);
        }
    }

    void sortedRows(NameCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameCountRows _rows;
};

// The int64-keyed sibling of GroupedCountSink, for a grouping key that is a numeric
// property rather than a name: a nullable i64 key chunk and a non-null ui64 count chunk.
class GroupedAgeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ages = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(ages, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& ageRaw = ages->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(ageRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(AgeCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    AgeCountRows _rows;
};

// The three-column sibling of GroupedCountSink: a nullable string key beside a plain
// count and a distinct count of the same column, so one query shows what the DISTINCT
// changes.
class GroupedCountPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        const auto* distincts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(distincts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& countRaw = counts->getRaw();
        const auto& distinctRaw = distincts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, countRaw[rowIndex], distinctRaw[rowIndex]);
        }
    }

    void sortedRows(NameCountCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameCountCountRows _rows;
};

// A value reduction beside a distinct count under the same key: a nullable string key,
// the nullable i64 sum of one column and the distinct count of another.
class GroupedSumCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& sumRaw = sums->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, sumRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(NameSumCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameSumCountRows _rows;
};

}

// End-to-end count(DISTINCT x) through the MLIR frontend: each query is parsed,
// analyzed, generated into the db dialect by DBProgramGenerator, then lowered and
// interpreted against the shared simpledb fixture. The scalar form generates a
// db.count carrying the distinct flag; the grouped form a db.group_aggregate whose
// kind is count_distinct.
class CountDistinctTest : public TuringTest {
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

    void runQuery(std::string_view query, NLOutputSink* sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory, chunkSize);
        interpreter.run();
    }

    void expectScalarCount(std::string_view query, uint64_t expected, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        ScalarCountSink sink;
        runQuery(query, &sink, chunkSize);

        EXPECT_EQ(sink.values(), (Counts {expected})) << "query: " << query;
    }

    void expectGroupedCounts(std::string_view query,
                             const NameCountRows& expected,
                             size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedCountSink sink;
        runQuery(query, &sink, chunkSize);

        NameCountRows rows;
        sink.sortedRows(rows);

        NameCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectAgeGroupedCounts(std::string_view query,
                                const AgeCountRows& expected,
                                size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedAgeCountSink sink;
        runQuery(query, &sink, chunkSize);

        AgeCountRows rows;
        sink.sortedRows(rows);

        AgeCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    // The db dialect program of a query, so a test can assert on the op the frontend
    // generated rather than only on the rows it produces
    std::string generatedProgram(std::string_view query) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module.get().print(stream);

        return printed;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The frontend turns count(DISTINCT b) into a db.count carrying the distinct flag,
// where count(b) generates the same op without it - so the flag is what the two
// queries differ by, all the way down to the lowering.
TEST_F(CountDistinctTest, generatesADistinctCount) {
    EXPECT_NE(generatedProgram("MATCH (a)-->(b) RETURN count(DISTINCT b)").find(") distinct :"), std::string::npos);
    EXPECT_EQ(generatedProgram("MATCH (a)-->(b) RETURN count(b)").find("distinct"), std::string::npos);
}

// The grouped form generates the count_distinct kind, where a plain grouped count
// generates count.
TEST_F(CountDistinctTest, generatesACountDistinctAggregateKind) {
    EXPECT_NE(generatedProgram("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b)").find("aggregates [count_distinct]"),
              std::string::npos);
    EXPECT_NE(generatedProgram("MATCH (a)-->(b) RETURN a.name, count(b)").find("aggregates [count]"),
              std::string::npos);
}

// Eighteen out-edges point at only twelve distinct nodes, so count(b) tallies the
// rows and count(DISTINCT b) the targets.
TEST_F(CountDistinctTest, countsDistinctTargetsOfAHop) {
    expectScalarCount("MATCH (a)-->(b) RETURN count(b)", 18);
    expectScalarCount("MATCH (a)-->(b) RETURN count(DISTINCT b)", 12);
}

// The twelve two-hop paths pass through only three middle nodes (Adam, Ghosts and
// Remy - the nodes with both an in-edge and an out-edge), so the DISTINCT collapses
// four rows into one for each.
TEST_F(CountDistinctTest, countsDistinctMiddleNodesOfATwoHop) {
    expectScalarCount("MATCH (a)-->(b)-->(c) RETURN count(b)", 12);
    expectScalarCount("MATCH (a)-->(b)-->(c) RETURN count(DISTINCT b)", 3);
}

// Only Remy and Adam carry an age, and both are 32, so count(a.age) charges the two
// present values and count(DISTINCT a.age) the one value they share. The nodes with
// no age are charged by neither: a null is dropped by the count, so it never becomes
// a distinct value of its own.
TEST_F(CountDistinctTest, countsDistinctPropertyValuesIgnoringNulls) {
    expectScalarCount("MATCH (a) RETURN count(a.age)", 2);
    expectScalarCount("MATCH (a) RETURN count(DISTINCT a.age)", 1);
}

// Nothing matches, so there is no value to charge and the count is zero - the same
// single row a plain count emits over an empty input.
TEST_F(CountDistinctTest, countsZeroWhenNothingMatches) {
    expectScalarCount("MATCH (a) WHERE a.age = 999 RETURN count(DISTINCT a)", 0);
    expectScalarCount("MATCH (a) WHERE a.age = 999 RETURN count(DISTINCT a.age)", 0);
}

// The argument holds a value in every matched row rather than one that varies with it,
// so count(42) charges each of simpledb's eighteen nodes while count(DISTINCT 42)
// charges the one value they all carry.
TEST_F(CountDistinctTest, countsOneDistinctValueOfAConstant) {
    expectScalarCount("MATCH (a) RETURN count(42)", 18);
    expectScalarCount("MATCH (a) RETURN count(DISTINCT 42)", 1);
}

// The rows a constant stands for are the rows the query left standing: a filter matching
// nothing leaves none to carry the value, so there is no distinct value to charge - while
// a projection with nothing driving it is one row of its own.
TEST_F(CountDistinctTest, countsAConstantOverAnEmptyAndAnAbsentMatch) {
    expectScalarCount("MATCH (a) WHERE a.age = 999 RETURN count(DISTINCT 42)", 0);
    expectScalarCount("RETURN count(DISTINCT 42)", 1);
}

// Two distinct counts in one projection: only Remy and Adam carry an age and both are
// 32 (one distinct value), while isFrench takes both true and false across the people
// (two). Each count builds its own seen-set - the nl.distinct ops must not be merged
// into one, which would make the second count charge against the first's set.
TEST_F(CountDistinctTest, keepsTwoDistinctCountsIndependent) {
    ScalarCountPairSink sink;
    runQuery("MATCH (a) RETURN count(DISTINCT a.age), count(DISTINCT a.isFrench)", &sink);

    EXPECT_EQ(sink.rows(), (CountPairs {{1, 2}}));
}

// A scan feeding a chunk size below the row count makes the same value arrive in
// different chunks. The seen-set is reset once at function scope, not per chunk, so
// the tally must not grow with the number of chunks.
TEST_F(CountDistinctTest, countsDistinctTargetsAcrossChunkBoundaries) {
    expectScalarCount("MATCH (a)-->(b) RETURN count(DISTINCT b)", 12, /*chunkSize=*/2);
    expectScalarCount("MATCH (a)-->(b)-->(c) RETURN count(DISTINCT b)", 3, /*chunkSize=*/1);
}

// Each two-hop source sees four (a, b) rows: Remy reaches Adam three times and Ghosts
// once, while Adam and Ghosts each reach Remy four times. So the plain count is four
// everywhere and the distinct count tells the groups apart.
TEST_F(CountDistinctTest, countsDistinctMiddleNodesPerGroup) {
    const NameCountRows plain {{"Adam", 4}, {"Ghosts", 4}, {"Remy", 4}};
    expectGroupedCounts("MATCH (a)-->(b)-->(c) RETURN a.name, count(b)", plain);

    const NameCountRows distinct {{"Adam", 1}, {"Ghosts", 1}, {"Remy", 2}};
    expectGroupedCounts("MATCH (a)-->(b)-->(c) RETURN a.name, count(DISTINCT b)", distinct);
}

// Grouping by source name and counting the distinct edge durations: Remy's three
// durations are all 20 (one distinct value), Luc's are 20 and 15 (two), and a source
// whose edges carry no duration counts zero.
TEST_F(CountDistinctTest, countsDistinctEdgePropertyValuesPerGroup) {
    const NameCountRows expected {
        {"Adam", 1}, {"Cyrus", 0}, {"Doruk", 0}, {"Ghosts", 1}, {"Luc", 2},
        {"Martina", 1}, {"Maxime", 0}, {"Remy", 1}, {"Suhas", 0},
    };

    expectGroupedCounts("MATCH (a)-[e]->(b) RETURN a.name, count(DISTINCT e.duration)", expected);
}

// Two aggregates over the same column in one projection: the plain count charges every
// present duration and the distinct count charges each value once, so the two
// accumulators must stay independent within a group.
TEST_F(CountDistinctTest, countsPlainAndDistinctSideBySide) {
    GroupedCountPairSink sink;
    runQuery("MATCH (a)-[e]->(b) RETURN a.name, count(e.duration), count(DISTINCT e.duration)", &sink);

    NameCountCountRows rows;
    sink.sortedRows(rows);

    NameCountCountRows expected;
    expected.emplace_back(std::optional<std::string>("Adam"), 1u, 1u);
    expected.emplace_back(std::optional<std::string>("Cyrus"), 0u, 0u);
    expected.emplace_back(std::optional<std::string>("Doruk"), 0u, 0u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 1u, 1u);
    expected.emplace_back(std::optional<std::string>("Luc"), 2u, 2u);
    expected.emplace_back(std::optional<std::string>("Martina"), 1u, 1u);
    expected.emplace_back(std::optional<std::string>("Maxime"), 0u, 0u);
    expected.emplace_back(std::optional<std::string>("Remy"), 3u, 1u);
    expected.emplace_back(std::optional<std::string>("Suhas"), 0u, 0u);
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows, expected);
}

// The DISTINCT argument is the grouping key itself, so a group cannot hold two values of
// it: simpledb's two 32-year-olds are one group charging the one age they share, and the
// sixteen nodes with no age are the null group - a key to group on, but not a value for
// the count to charge.
TEST_F(CountDistinctTest, countsTheDistinctValuesOfTheGroupingKey) {
    const AgeCountRows expected {{32, 1}, {std::nullopt, 0}};

    expectAgeGroupedCounts("MATCH (a) RETURN a.age, count(DISTINCT a.age)", expected);
}

// The same key one step up: an expression over two matched nodes, aliased, then counted
// through that alias. Only Remy and Adam carry an age, so of the 324 pairs of the product
// just the four among those two have a difference at all - and all four differ by zero.
TEST_F(CountDistinctTest, countsTheDistinctValuesOfAnExpressionKey) {
    const AgeCountRows expected {{0, 1}, {std::nullopt, 0}};

    expectAgeGroupedCounts("MATCH (a), (b) RETURN b.age - a.age AS diff, COUNT(DISTINCT diff)", expected);
}

// A value reduction and a distinct count in one grouped projection, over two different
// columns: each aggregate carries its own accumulator and its own seen-set, so the sum of
// the durations and the tally of the targets are folded side by side under one key. A
// source whose edges carry no duration sums to zero, the additive identity.
TEST_F(CountDistinctTest, countsDistinctBesideASumPerGroup) {
    GroupedSumCountSink sink;
    runQuery("MATCH (a)-[e]->(b) RETURN a.name, sum(e.duration), count(DISTINCT b)", &sink);

    NameSumCountRows rows;
    sink.sortedRows(rows);

    NameSumCountRows expected;
    expected.emplace_back(std::optional<std::string>("Adam"), 20, 3u);
    expected.emplace_back(std::optional<std::string>("Cyrus"), 0, 2u);
    expected.emplace_back(std::optional<std::string>("Doruk"), 0, 1u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 200, 1u);
    expected.emplace_back(std::optional<std::string>("Luc"), 35, 2u);
    expected.emplace_back(std::optional<std::string>("Martina"), 10, 1u);
    expected.emplace_back(std::optional<std::string>("Maxime"), 0, 2u);
    expected.emplace_back(std::optional<std::string>("Remy"), 60, 4u);
    expected.emplace_back(std::optional<std::string>("Suhas"), 0, 2u);
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows, expected);
}

// A chunk size below the group's row count spreads one group's rows over several
// chunks. The per-group seen-set is reset with the rest of the accumulator, once at
// function scope, so a value repeated in a later chunk must still not be charged
// twice.
TEST_F(CountDistinctTest, countsDistinctPerGroupAcrossChunkBoundaries) {
    const NameCountRows expected {{"Adam", 1}, {"Ghosts", 1}, {"Remy", 2}};

    expectGroupedCounts("MATCH (a)-->(b)-->(c) RETURN a.name, count(DISTINCT b)", expected, /*chunkSize=*/1);
    expectGroupedCounts("MATCH (a)-->(b)-->(c) RETURN a.name, count(DISTINCT b)", expected, /*chunkSize=*/3);
}

// DISTINCT is a modifier of count alone for now: a value reduction carrying it is
// still turned away, rather than silently reducing the repeated values.
TEST_F(CountDistinctTest, rejectsDistinctOnAValueReduction) {
    ScalarCountSink sink;
    EXPECT_THROW(runQuery("MATCH (a) RETURN sum(DISTINCT a.age)", &sink), TuringException);
    EXPECT_THROW(runQuery("MATCH (a) RETURN a.name, avg(DISTINCT a.age)", &sink), TuringException);
}
