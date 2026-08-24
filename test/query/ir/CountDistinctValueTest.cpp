#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

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
using FlagCountRow = std::pair<std::optional<bool>, uint64_t>;
using FlagCountRows = std::vector<FlagCountRow>;
using DurationCountRow = std::pair<std::optional<int64_t>, uint64_t>;
using DurationCountRows = std::vector<DurationCountRow>;

class ScalarCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(countRaw[rowIndex]);
        }
    }

    const Counts& values() const { return _values; }

private:
    Counts _values;
};

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

// The boolean-keyed sibling of GroupedCountSink. A key cell is narrowed from CustomBool
// to bool so the collected rows order and compare by the value the key stands for.
class GroupedFlagCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* flags = dynamic_cast<const ColumnOptVector<CustomBool>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(flags, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& flagRaw = flags->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<bool> flag;
            if (flagRaw[rowIndex]) {
                flag = static_cast<bool>(*flagRaw[rowIndex]);
            }

            _rows.emplace_back(flag, countRaw[rowIndex]);
        }
    }

    void sortedRows(FlagCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    FlagCountRows _rows;
};

class GroupedDurationCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* durations = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(durations, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& durationRaw = durations->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(durationRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(DurationCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    DurationCountRows _rows;
};

}

// The value domain of count(DISTINCT x) end to end: every property type simpledb carries,
// the nulls the count must drop, the degenerate inputs, and the chunk boundaries a value
// has to be recognised across - the chunk size is the fixture's, so a value seen in one
// chunk is looked up again in the next.
class CountDistinctValueTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            _graph = system.createGraph(_graphName);
        }

        SimpleGraph::createSimpleGraph(_graph);

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            _emptyGraph = system.createGraph(_emptyGraphName);
        }
    }

    void runQueryOn(Graph* graph, std::string_view query, NLOutputSink* sink, size_t chunkSize) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const ProcedureManager* procedures = system.getProcedures();

            const FrozenCommitTx transaction = graph->openTransaction();
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

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory, chunkSize);
        interpreter.run();
    }

    void expectScalarCount(std::string_view query, uint64_t expected, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        ScalarCountSink sink;
        runQueryOn(_graph, query, &sink, chunkSize);

        EXPECT_EQ(sink.values(), (Counts {expected})) << "query: " << query;
    }

    void expectScalarCountOnEmptyGraph(std::string_view query, uint64_t expected) {
        ScalarCountSink sink;
        runQueryOn(_emptyGraph, query, &sink, ChunkConfig::CHUNK_SIZE);

        EXPECT_EQ(sink.values(), (Counts {expected})) << "query: " << query;
    }

    void expectCountPair(std::string_view query,
                         uint64_t plain,
                         uint64_t distinct,
                         size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        ScalarCountPairSink sink;
        runQueryOn(_graph, query, &sink, chunkSize);

        EXPECT_EQ(sink.rows(), (CountPairs {{plain, distinct}})) << "query: " << query;
    }

    void expectGroupedCounts(std::string_view query,
                             const NameCountRows& expected,
                             size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedCountSink sink;
        runQueryOn(_graph, query, &sink, chunkSize);

        NameCountRows rows;
        sink.sortedRows(rows);

        NameCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query << ", chunk size: " << chunkSize;
    }

    void expectFlagGroupedCounts(std::string_view query,
                                 const FlagCountRows& expected,
                                 size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedFlagCountSink sink;
        runQueryOn(_graph, query, &sink, chunkSize);

        FlagCountRows rows;
        sink.sortedRows(rows);

        FlagCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query << ", chunk size: " << chunkSize;
    }

    void expectDurationGroupedCounts(std::string_view query,
                                     const DurationCountRows& expected,
                                     size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedDurationCountSink sink;
        runQueryOn(_graph, query, &sink, chunkSize);

        DurationCountRows rows;
        sink.sortedRows(rows);

        DurationCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query << ", chunk size: " << chunkSize;
    }

    const std::string _graphName = "simpledb";
    const std::string _emptyGraphName = "emptydb";
    const std::vector<size_t> _chunkSizes {1, 2, 3, 7};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    Graph* _emptyGraph {nullptr};
};

// Every one of the eighteen nodes carries a name and no two share one, so the string keys
// never fold; only Remy, Adam, Maxime and Luc carry a dob, and those four differ too.
TEST_F(CountDistinctValueTest, countsDistinctStringPropertyValues) {
    expectCountPair("MATCH (a) RETURN count(a.name), count(DISTINCT a.name)", 18, 18);
    expectCountPair("MATCH (a) RETURN count(a.dob), count(DISTINCT a.dob)", 4, 4);
}

// A boolean column holds at most the two values, so eight people carrying isFrench charge
// two distinct values, and so do the seven interests carrying isReal.
TEST_F(CountDistinctValueTest, countsDistinctBooleanPropertyValues) {
    expectCountPair("MATCH (a) RETURN count(a.isFrench), count(DISTINCT a.isFrench)", 8, 2);
    expectCountPair("MATCH (a) RETURN count(a.hasPhD), count(DISTINCT a.hasPhD)", 8, 2);
    expectCountPair("MATCH (a:Interest) RETURN count(a.isReal), count(DISTINCT a.isReal)", 7, 2);
}

// The eight durations halve to 10.0, 100.0, 7.5 and 5.0, so a double key folds the four
// twenties into one exactly as the integer column does.
TEST_F(CountDistinctValueTest, countsDistinctDoubleValues) {
    expectCountPair("MATCH (a)-[e]->(b) RETURN count(e.duration * 0.5), count(DISTINCT e.duration * 0.5)", 8, 4);
}

// Eight of the eighteen edges carry a duration and those hold 20, 200, 15 and 10; five
// carry a proficiency and those hold expert and moderate; all eighteen carry a distinct
// name.
TEST_F(CountDistinctValueTest, countsDistinctEdgePropertyValues) {
    expectCountPair("MATCH (a)-[e]->(b) RETURN count(e.duration), count(DISTINCT e.duration)", 8, 4);
    expectCountPair("MATCH (a)-[e]->(b) RETURN count(e.proficiency), count(DISTINCT e.proficiency)", 5, 2);
    expectCountPair("MATCH (a)-[e]->(b) RETURN count(e.name), count(DISTINCT e.name)", 18, 18);
}

// The eighteen out-edges leave from nine distinct sources, and no edge repeats.
TEST_F(CountDistinctValueTest, countsDistinctEntityIdentities) {
    expectCountPair("MATCH (a)-[e]->(b) RETURN count(DISTINCT a), count(DISTINCT e)", 9, 18);
}

// Only Remy and Adam carry an age, so each of the six other people is a group whose every
// row is null - a group to emit, but no value to charge.
TEST_F(CountDistinctValueTest, chargesNoDistinctValueForAGroupOfOnlyNulls) {
    const NameCountRows expected {
        {"Remy", 1}, {"Adam", 1}, {"Maxime", 0}, {"Luc", 0},
        {"Martina", 0}, {"Suhas", 0}, {"Cyrus", 0}, {"Doruk", 0},
    };

    expectGroupedCounts("MATCH (a:Person) RETURN a.name, count(DISTINCT a.age)", expected);
}

// Remy's four out-edges carry no proficiency, expert, expert and moderate, so the null is
// dropped and the repeat folded, leaving two. Maxime and Ghosts each have one present
// value among their edges, and the rest none.
TEST_F(CountDistinctValueTest, countsDistinctValuesMixedWithNullsInOneGroup) {
    const NameCountRows expected {
        {"Remy", 2}, {"Adam", 0}, {"Ghosts", 1}, {"Maxime", 1}, {"Luc", 0},
        {"Martina", 0}, {"Cyrus", 0}, {"Suhas", 0}, {"Doruk", 0},
    };

    expectGroupedCounts("MATCH (a)-[e]->(b) RETURN a.name, count(DISTINCT e.proficiency)", expected);
}

// A null is a grouping key of its own: the ten edges carrying no duration come from six
// distinct sources, while the five edges of duration 20 come from Remy, Adam and Luc.
TEST_F(CountDistinctValueTest, countsDistinctValuesUnderANullGroupingKey) {
    const DurationCountRows expected {{20, 3}, {200, 1}, {15, 1}, {10, 1}, {std::nullopt, 6}};

    expectDurationGroupedCounts("MATCH (a)-[e]->(b) RETURN e.duration, count(DISTINCT a.name)", expected);
}

// Both boolean groups hold a PhD and a non-PhD, and the ten interests carry neither
// property - one null group charging nothing.
TEST_F(CountDistinctValueTest, countsDistinctBooleanValuesPerGroup) {
    const FlagCountRows expected {{true, 2}, {false, 2}, {std::nullopt, 0}};

    expectFlagGroupedCounts("MATCH (a) RETURN a.isFrench, count(DISTINCT a.hasPhD)", expected);
}

TEST_F(CountDistinctValueTest, countsDistinctValuesOverASingleMatchedRow) {
    expectCountPair("MATCH (a) WHERE a.name = 'Remy' RETURN count(DISTINCT a.name), count(DISTINCT a.age)", 1, 1);
}

// A grouped projection emits one row per group, so an input with no rows has no group to
// emit - unlike the scalar count, which always emits its single tally.
TEST_F(CountDistinctValueTest, emitsNoGroupWhenNothingMatches) {
    expectGroupedCounts("MATCH (a) WHERE a.age = 999 RETURN a.name, count(DISTINCT a.name)", NameCountRows {});
    expectDurationGroupedCounts("MATCH (a) WHERE a.age = 999 RETURN a.age, count(DISTINCT a.name)",
                                DurationCountRows {});
}

// The product pairs each of the eighteen nodes with each other, so a node stands in
// eighteen rows and its name and age are seen once per row.
TEST_F(CountDistinctValueTest, countsDistinctValuesOverACrossProduct) {
    expectCountPair("MATCH (a), (b) RETURN count(a), count(DISTINCT a)", 324, 18);
    expectCountPair("MATCH (a), (b) RETURN count(DISTINCT b.name), count(DISTINCT b.age)", 18, 1);
}

TEST_F(CountDistinctValueTest, countsZeroOverAnEmptyGraph) {
    expectScalarCountOnEmptyGraph("MATCH (a) RETURN count(DISTINCT a)", 0);
    expectScalarCountOnEmptyGraph("MATCH (a) RETURN count(DISTINCT 42)", 0);
    expectScalarCountOnEmptyGraph("MATCH (a), (b) RETURN count(DISTINCT a)", 0);
}

// A chunk size below the row count makes the same string arrive again in a later chunk:
// expert is carried by four edges spread over three of simpledb's data parts, and the
// seen-set must recognise it every time after the first.
TEST_F(CountDistinctValueTest, countsDistinctStringValuesAcrossChunkBoundaries) {
    for (const size_t chunkSize : _chunkSizes) {
        expectScalarCount("MATCH (a) RETURN count(DISTINCT a.name)", 18, chunkSize);
        expectScalarCount("MATCH (a)-[e]->(b) RETURN count(DISTINCT e.proficiency)", 2, chunkSize);
    }
}

TEST_F(CountDistinctValueTest, countsDistinctBooleanValuesAcrossChunkBoundaries) {
    for (const size_t chunkSize : _chunkSizes) {
        expectScalarCount("MATCH (a) RETURN count(DISTINCT a.isFrench)", 2, chunkSize);
        expectScalarCount("MATCH (a:Interest) RETURN count(DISTINCT a.isReal)", 2, chunkSize);
    }
}

TEST_F(CountDistinctValueTest, countsDistinctDoubleValuesAcrossChunkBoundaries) {
    for (const size_t chunkSize : _chunkSizes) {
        expectScalarCount("MATCH (a)-[e]->(b) RETURN count(DISTINCT e.duration * 0.5)", 4, chunkSize);
    }
}

// Remy's four edges land in different chunks at these sizes, so his group is folded a row
// at a time and his two proficiencies must not be charged twice.
TEST_F(CountDistinctValueTest, countsDistinctStringValuesPerGroupAcrossChunkBoundaries) {
    const NameCountRows expected {
        {"Remy", 2}, {"Adam", 0}, {"Ghosts", 1}, {"Maxime", 1}, {"Luc", 0},
        {"Martina", 0}, {"Cyrus", 0}, {"Suhas", 0}, {"Doruk", 0},
    };

    for (const size_t chunkSize : _chunkSizes) {
        expectGroupedCounts("MATCH (a)-[e]->(b) RETURN a.name, count(DISTINCT e.proficiency)", expected, chunkSize);
    }
}

TEST_F(CountDistinctValueTest, countsDistinctBooleanValuesPerGroupAcrossChunkBoundaries) {
    const FlagCountRows expected {{true, 2}, {false, 2}, {std::nullopt, 0}};

    for (const size_t chunkSize : _chunkSizes) {
        expectFlagGroupedCounts("MATCH (a) RETURN a.isFrench, count(DISTINCT a.hasPhD)", expected, chunkSize);
    }
}

TEST_F(CountDistinctValueTest, countsDistinctValuesUnderANullGroupingKeyAcrossChunkBoundaries) {
    const DurationCountRows expected {{20, 3}, {200, 1}, {15, 1}, {10, 1}, {std::nullopt, 6}};

    for (const size_t chunkSize : _chunkSizes) {
        expectDurationGroupedCounts("MATCH (a)-[e]->(b) RETURN e.duration, count(DISTINCT a.name)",
                                    expected,
                                    chunkSize);
    }
}

// An unwound list is one Cypher value per cell whatever its type, so 1 and 1.0 are the
// same value, a string and a boolean are two more, and the nulls are dropped rather than
// charged as a value of their own.
TEST_F(CountDistinctValueTest, countsDistinctValuesOfAHeterogeneousList) {
    expectScalarCount("UNWIND [1, 1.0, 2] AS x RETURN count(DISTINCT x)", 2);
    expectCountPair("UNWIND [1, 'a', 1, 'a', true] AS x RETURN count(x), count(DISTINCT x)", 5, 3);
    expectCountPair("UNWIND [1, null, 1, null] AS x RETURN count(x), count(DISTINCT x)", 2, 1);
}

// The same heterogeneous cells under a grouping key: each founder sees 1, 'a' and 1, so
// each group charges the integer once and the string once.
TEST_F(CountDistinctValueTest, countsDistinctValuesOfAHeterogeneousListPerGroup) {
    const NameCountRows expected {{"Remy", 2}, {"Adam", 2}};

    expectGroupedCounts("MATCH (a:Founder) UNWIND [1, 'a', 1] AS x RETURN a.name, count(DISTINCT x)", expected);
}

// count ignores nulls, so a null argument charges nothing at all: zero over a match of
// eighteen rows and zero over the single row of a projection with no match.
TEST_F(CountDistinctValueTest, countsNoDistinctValueOfANullLiteral) {
    expectScalarCount("MATCH (a) RETURN count(DISTINCT null)", 0);
    expectScalarCount("RETURN count(DISTINCT null)", 0);
}
