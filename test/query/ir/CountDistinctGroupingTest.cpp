#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
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
#include "columns/ColumnIDs.h"
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
using NameCountRow = std::pair<std::optional<std::string>, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;
using ValueCountRow = std::pair<std::optional<int64_t>, uint64_t>;
using ValueCountRows = std::vector<ValueCountRow>;
using NameValueCountRow = std::tuple<std::optional<std::string>, std::optional<int64_t>, uint64_t>;
using NameValueCountRows = std::vector<NameValueCountRow>;
using NameAverageCountRow = std::tuple<std::optional<std::string>, std::optional<double>, uint64_t>;
using NameAverageCountRows = std::vector<NameAverageCountRow>;
using NameCountCountRow = std::tuple<std::optional<std::string>, uint64_t, uint64_t>;
using NameCountCountRows = std::vector<NameCountCountRow>;
using CountNameRow = std::pair<uint64_t, std::optional<std::string>>;
using CountNameRows = std::vector<CountNameRow>;
using IDCountRow = std::pair<uint64_t, uint64_t>;
using IDCountRows = std::vector<IDCountRow>;
using IDPairCountRow = std::tuple<uint64_t, uint64_t, uint64_t>;
using IDPairCountRows = std::vector<IDPairCountRow>;

const std::vector<std::string> allNodeNames {
    "Remy", "Adam", "Computers", "Eighties", "Bio", "Cooking",
    "Ghosts", "Padel", "Maxime", "Luc", "Animals", "Martina",
    "Suhas", "Gym", "Travel", "Cyrus", "JiuJitsu", "Doruk",
};

// Collects the single-column ui64 rows a scalar count emits.
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

// Collects the (name, count) rows a count grouped by one string key emits. A grouped
// aggregate emits its groups in first-seen order, which the language does not promise,
// so every test here compares sorted row sets.
class GroupedNameCountSink : public NLOutputSink {
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

// The int64-keyed sibling of GroupedNameCountSink, for a key that is a numeric
// expression or an unwound number rather than a name.
class GroupedValueCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(values, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& valueRaw = values->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(valueRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(ValueCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    ValueCountRows _rows;
};

// Collects the (name, value, count) rows a distinct count emits beside a nullable i64
// column: a second grouping key, or the reduction of a value column under one key.
class GroupedNameValueCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& valueRaw = values->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, valueRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(NameValueCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameValueCountRows _rows;
};

// The f64 sibling of GroupedNameValueCountSink: avg widens its input to a double, so a
// distinct count standing beside it reads a nullable f64 middle column.
class GroupedNameAverageCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* averages = dynamic_cast<const ColumnOptVector<double>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(averages, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& averageRaw = averages->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, averageRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    void sortedRows(NameAverageCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameAverageCountRows _rows;
};

// Collects the (name, count, count) rows two tallies under one string key emit.
class GroupedNameCountCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* firsts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        const auto* seconds = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(firsts, nullptr);
        ASSERT_NE(seconds, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& firstRaw = firsts->getRaw();
        const auto& secondRaw = seconds->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, firstRaw[rowIndex], secondRaw[rowIndex]);
        }
    }

    void sortedRows(NameCountCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameCountCountRows _rows;
};

// Collects the (count, name) rows a projection that names its distinct count before its
// grouping key emits, so a test can show the output keeps the projection's column order
// rather than the order the grouped aggregate produces its results in.
class GroupedCountNameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(names, nullptr);

        const auto& countRaw = counts->getRaw();
        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(countRaw[rowIndex], name);
        }
    }

    void sortedRows(CountNameRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    CountNameRows _rows;
};

// Collects the (node, count) rows a count grouped by a bare node variable emits: the key
// is the node ID column itself, not a property read off it.
class GroupedNodeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*nodes)[rowIndex].getValue(), countRaw[rowIndex]);
        }
    }

    void sortedRows(IDCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    IDCountRows _rows;
};

// The two-key sibling of GroupedNodeCountSink: a count grouped by a pair of node
// variables, so both key columns are ID columns.
class GroupedNodePairCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* sources = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* middles = dynamic_cast<const ColumnNodeIDs*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(sources, nullptr);
        ASSERT_NE(middles, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*sources)[rowIndex].getValue(),
                               (*middles)[rowIndex].getValue(),
                               countRaw[rowIndex]);
        }
    }

    void sortedRows(IDPairCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    IDPairCountRows _rows;
};

// Collects the (edge, count) rows a count grouped by a bare edge variable emits.
class GroupedEdgeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* edges = dynamic_cast<const ColumnEdgeIDs*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(edges, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*edges)[rowIndex].getValue(), countRaw[rowIndex]);
        }
    }

    void sortedRows(IDCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    IDCountRows _rows;
};

}

// The grouping shape a count(DISTINCT x) is folded under: several keys, a key that is an
// expression or a bare node or edge variable, other aggregates beside it, and the
// multi-clause pipelines the v3 frontend supports (a second MATCH, an UNWIND). Every
// query is parsed, analyzed, generated into the db dialect, lowered and interpreted
// against the shared simpledb fixture, and each is stated beside the plain count(x) it
// only differs from by the DISTINCT.
class CountDistinctGroupingTest : public TuringTest {
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

    void expectScalarCount(std::string_view query, uint64_t expected) {
        ScalarCountSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (Counts {expected})) << "query: " << query;
    }

    void expectNameCounts(std::string_view query,
                          const NameCountRows& expected,
                          size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedNameCountSink sink;
        runQuery(query, &sink, chunkSize);

        NameCountRows rows;
        sink.sortedRows(rows);

        NameCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectValueCounts(std::string_view query, const ValueCountRows& expected) {
        GroupedValueCountSink sink;
        runQuery(query, &sink);

        ValueCountRows rows;
        sink.sortedRows(rows);

        ValueCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectNameValueCounts(std::string_view query,
                               const NameValueCountRows& expected,
                               size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        GroupedNameValueCountSink sink;
        runQuery(query, &sink, chunkSize);

        NameValueCountRows rows;
        sink.sortedRows(rows);

        NameValueCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectNameAverageCounts(std::string_view query, const NameAverageCountRows& expected) {
        GroupedNameAverageCountSink sink;
        runQuery(query, &sink);

        NameAverageCountRows rows;
        sink.sortedRows(rows);

        NameAverageCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectNameCountCounts(std::string_view query, const NameCountCountRows& expected) {
        GroupedNameCountCountSink sink;
        runQuery(query, &sink);

        NameCountCountRows rows;
        sink.sortedRows(rows);

        NameCountCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectCountNames(std::string_view query, const CountNameRows& expected) {
        GroupedCountNameSink sink;
        runQuery(query, &sink);

        CountNameRows rows;
        sink.sortedRows(rows);

        CountNameRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectNodeCounts(std::string_view query, const IDCountRows& expected) {
        GroupedNodeCountSink sink;
        runQuery(query, &sink);

        IDCountRows rows;
        sink.sortedRows(rows);

        IDCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectNodePairCounts(std::string_view query, const IDPairCountRows& expected) {
        GroupedNodePairCountSink sink;
        runQuery(query, &sink);

        IDPairCountRows rows;
        sink.sortedRows(rows);

        IDPairCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectEdgeCounts(std::string_view query, const IDCountRows& expected) {
        GroupedEdgeCountSink sink;
        runQuery(query, &sink);

        IDCountRows rows;
        sink.sortedRows(rows);

        IDCountRows sortedExpected = expected;
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

// One db.group_aggregate carries every key and every aggregate of the projection, so two
// keys beside two distinct counts is one op with a key count of two and one
// count_distinct kind per tally - and a distinct count mixes with the other kinds in the
// order the projection names them.
TEST_F(CountDistinctGroupingTest, generatesOneAggregateForEveryKeyAndTally) {
    const std::string twoKeys =
        generatedProgram("MATCH (a)-->(b)-->(c) RETURN a.name, a.age, count(DISTINCT b), count(DISTINCT c)");
    EXPECT_NE(twoKeys.find("keys 2 aggregates [count_distinct, count_distinct]"), std::string::npos);

    const std::string mixed =
        generatedProgram("MATCH (a)-[e]->(b) RETURN a.name, count(*), sum(e.duration), count(DISTINCT b)");
    EXPECT_NE(mixed.find("keys 1 aggregates [count, sum, count_distinct]"), std::string::npos);
}

// Two keys beside the tally: the four two-hop rows of each source are one group whatever
// the second key adds, since a source's name and age never disagree.
TEST_F(CountDistinctGroupingTest, groupsByTwoKeys) {
    NameValueCountRows plain;
    plain.emplace_back(std::optional<std::string>("Remy"), 32, 4u);
    plain.emplace_back(std::optional<std::string>("Adam"), 32, 4u);
    plain.emplace_back(std::optional<std::string>("Ghosts"), std::nullopt, 4u);
    expectNameValueCounts("MATCH (a)-->(b)-->(c) RETURN a.name, a.age, count(b)", plain);

    NameValueCountRows distinct;
    distinct.emplace_back(std::optional<std::string>("Remy"), 32, 2u);
    distinct.emplace_back(std::optional<std::string>("Adam"), 32, 1u);
    distinct.emplace_back(std::optional<std::string>("Ghosts"), std::nullopt, 1u);
    expectNameValueCounts("MATCH (a)-->(b)-->(c) RETURN a.name, a.age, count(DISTINCT b)", distinct);
}

// The key is an expression over the key node rather than a property of it. Only Remy and
// Adam carry an age, so their seven out-edges are one group reaching seven distinct
// targets, and the eleven out-edges of the ageless sources are the null group, reaching
// nine.
TEST_F(CountDistinctGroupingTest, groupsByAnExpressionKey) {
    expectValueCounts("MATCH (a)-->(b) RETURN a.age + 1, count(b)", ValueCountRows {{33, 7}, {std::nullopt, 11}});
    expectValueCounts("MATCH (a)-->(b) RETURN a.age + 1, count(DISTINCT b)",
                      ValueCountRows {{33, 7}, {std::nullopt, 9}});
}

// Both the key and the tally are aliased, so the projection names neither by the
// expression it computes.
TEST_F(CountDistinctGroupingTest, groupsByAnAliasedKey) {
    const NameCountRows expected {{"Adam", 1}, {"Ghosts", 1}, {"Remy", 2}};

    expectNameCounts("MATCH (a)-->(b)-->(c) RETURN a.name AS source, count(DISTINCT b) AS distinctMiddles",
                     expected);
}

// The count reads the key through its alias, one expression up: the group of the two
// 32-year-olds carries the one value 33, and the null key group carries none at all
// since a null plus one is still null.
TEST_F(CountDistinctGroupingTest, countsDistinctValuesOfAnExpressionOverAKeyAlias) {
    expectValueCounts("MATCH (a)-->(b) RETURN a.age AS age, count(DISTINCT age + 1)",
                      ValueCountRows {{32, 1}, {std::nullopt, 0}});
}

// The key is the node variable itself, so the groups are node IDs: Remy (0) passes
// through Adam (1) and Ghosts (6), while Adam and Ghosts both pass only through Remy (0).
TEST_F(CountDistinctGroupingTest, groupsByANodeVariable) {
    expectNodeCounts("MATCH (a)-->(b)-->(c) RETURN a, count(b)", IDCountRows {{0, 4}, {1, 4}, {6, 4}});
    expectNodeCounts("MATCH (a)-->(b)-->(c) RETURN a, count(DISTINCT b)", IDCountRows {{0, 2}, {1, 1}, {6, 1}});
}

// Two node variables as keys: each (source, middle) pair is its own group, and the tally
// is the out-degree of the middle - three for Adam (1), one for Ghosts (6), four for
// Remy (0).
TEST_F(CountDistinctGroupingTest, groupsByTwoNodeVariables) {
    const IDPairCountRows expected {{0, 1, 3}, {0, 6, 1}, {1, 0, 4}, {6, 0, 4}};

    expectNodePairCounts("MATCH (a)-->(b)-->(c) RETURN a, b, count(DISTINCT c)", expected);
}

// An edge variable is as much a grouping key as a node variable: each of simpledb's
// eighteen edges is its own group, reaching the one target it points at.
TEST_F(CountDistinctGroupingTest, groupsByAnEdgeVariable) {
    IDCountRows expected;
    for (uint64_t edge = 0; edge < 18; edge++) {
        expected.emplace_back(edge, 1u);
    }

    expectEdgeCounts("MATCH (a)-[e]->(b) RETURN e, count(b)", expected);
    expectEdgeCounts("MATCH (a)-[e]->(b) RETURN e, count(DISTINCT b)", expected);
}

// The projection names the tally before the key it is folded under, where the grouped
// aggregate produces its keys before its tallies, so the output has to follow the
// projection rather than the op.
TEST_F(CountDistinctGroupingTest, keepsTheDistinctCountBeforeItsKey) {
    const CountNameRows expected {{1, "Adam"}, {1, "Ghosts"}, {2, "Remy"}};

    expectCountNames("MATCH (a)-->(b)-->(c) RETURN count(DISTINCT b), a.name", expected);
}

// A value reduction beside the tally, over another column: Luc's two durations are 20
// and 15, so his minimum is 15 while his distinct targets are two, and a source whose
// edges carry no duration has no minimum at all.
TEST_F(CountDistinctGroupingTest, countsDistinctBesideAMinimum) {
    NameValueCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 20, 4u);
    expected.emplace_back(std::optional<std::string>("Adam"), 20, 3u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 200, 1u);
    expected.emplace_back(std::optional<std::string>("Maxime"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Luc"), 15, 2u);
    expected.emplace_back(std::optional<std::string>("Martina"), 10, 1u);
    expected.emplace_back(std::optional<std::string>("Cyrus"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Suhas"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Doruk"), std::nullopt, 1u);

    expectNameValueCounts("MATCH (a)-[e]->(b) RETURN a.name, min(e.duration), count(DISTINCT b)", expected);
}

// avg widens its input to a double where the tally stays an unsigned integer, so the two
// aggregates emit different column types under one key. Luc averages 20 and 15 to 17.5;
// a source with no duration averages nothing.
TEST_F(CountDistinctGroupingTest, countsDistinctBesideAnAverage) {
    NameAverageCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 20.0, 4u);
    expected.emplace_back(std::optional<std::string>("Adam"), 20.0, 3u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 200.0, 1u);
    expected.emplace_back(std::optional<std::string>("Maxime"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Luc"), 17.5, 2u);
    expected.emplace_back(std::optional<std::string>("Martina"), 10.0, 1u);
    expected.emplace_back(std::optional<std::string>("Cyrus"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Suhas"), std::nullopt, 2u);
    expected.emplace_back(std::optional<std::string>("Doruk"), std::nullopt, 1u);

    expectNameAverageCounts("MATCH (a)-[e]->(b) RETURN a.name, avg(e.duration), count(DISTINCT b)", expected);
}

// Two distinct counts under one key, over different columns: each keeps its own seen-set,
// so Remy's four distinct targets stand beside the one duration his edges share, and a
// source whose edges carry no duration tallies its targets and no duration at all.
TEST_F(CountDistinctGroupingTest, countsTwoDistinctColumnsUnderOneKey) {
    NameCountCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 4u, 1u);
    expected.emplace_back(std::optional<std::string>("Adam"), 3u, 1u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 1u, 1u);
    expected.emplace_back(std::optional<std::string>("Maxime"), 2u, 0u);
    expected.emplace_back(std::optional<std::string>("Luc"), 2u, 2u);
    expected.emplace_back(std::optional<std::string>("Martina"), 1u, 1u);
    expected.emplace_back(std::optional<std::string>("Cyrus"), 2u, 0u);
    expected.emplace_back(std::optional<std::string>("Suhas"), 2u, 0u);
    expected.emplace_back(std::optional<std::string>("Doruk"), 1u, 0u);

    expectNameCountCounts("MATCH (a)-[e]->(b) RETURN a.name, count(DISTINCT b), count(DISTINCT e.duration)",
                          expected);
}

// The two distinct counts are over the two edge variables of a two-hop, so both tally an
// ID column rather than a value one. Remy takes two first hops and reaches four second
// ones; Adam and Ghosts take one first hop each, into Remy's four out-edges.
TEST_F(CountDistinctGroupingTest, countsTwoDistinctEdgeColumnsUnderOneKey) {
    NameCountCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 2u, 4u);
    expected.emplace_back(std::optional<std::string>("Adam"), 1u, 4u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 1u, 4u);

    expectNameCountCounts("MATCH (a)-[e]->(b)-[f]->(c) RETURN a.name, count(DISTINCT e), count(DISTINCT f)",
                          expected);
}

// count(*) charges every row of the group where the distinct count charges each middle
// node once, so the two tallies differ on the same input.
TEST_F(CountDistinctGroupingTest, countsDistinctBesideACountStar) {
    NameCountCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 4u, 2u);
    expected.emplace_back(std::optional<std::string>("Adam"), 4u, 1u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), 4u, 1u);

    expectNameCountCounts("MATCH (a)-->(b)-->(c) RETURN a.name, count(*), count(DISTINCT b)", expected);
}

// The argument holds the same value in every row of every group, so the plain count is
// each source's out-degree and the distinct count the one value the group carries.
TEST_F(CountDistinctGroupingTest, countsDistinctValuesOfAConstantPerGroup) {
    const NameCountRows plain {
        {"Adam", 3}, {"Cyrus", 2}, {"Doruk", 1}, {"Ghosts", 1}, {"Luc", 2},
        {"Martina", 1}, {"Maxime", 2}, {"Remy", 4}, {"Suhas", 2},
    };
    expectNameCounts("MATCH (a)-->(b) RETURN a.name, count(42)", plain);

    const NameCountRows distinct {
        {"Adam", 1}, {"Cyrus", 1}, {"Doruk", 1}, {"Ghosts", 1}, {"Luc", 1},
        {"Martina", 1}, {"Maxime", 1}, {"Remy", 1}, {"Suhas", 1},
    };
    expectNameCounts("MATCH (a)-->(b) RETURN a.name, count(DISTINCT 42)", distinct);
}

// The hops are two MATCH clauses rather than one pattern, so the aggregation stands at
// the end of a two-stage pipeline. Remy's two middles reach three distinct nodes between
// them, and Adam and Ghosts each reach Remy's four.
TEST_F(CountDistinctGroupingTest, groupsAcrossASecondMatchClause) {
    expectNameCounts("MATCH (a)-->(b) MATCH (b)-->(c) RETURN a.name, count(c)",
                     NameCountRows {{"Adam", 4}, {"Ghosts", 4}, {"Remy", 4}});
    expectNameCounts("MATCH (a)-->(b) MATCH (b)-->(c) RETURN a.name, count(DISTINCT c)",
                     NameCountRows {{"Adam", 4}, {"Ghosts", 4}, {"Remy", 3}});
}

// The rows come from an UNWIND rather than a traversal: three elements carrying two
// values.
TEST_F(CountDistinctGroupingTest, countsDistinctOfAnUnwoundList) {
    expectScalarCount("UNWIND [1, 1, 2] AS x RETURN count(x)", 3);
    expectScalarCount("UNWIND [1, 1, 2] AS x RETURN count(DISTINCT x)", 2);
}

// The unwound element is the grouping key as well as the counted value, so a group holds
// one value and charges it once - the repeated element folds into its own group.
TEST_F(CountDistinctGroupingTest, groupsByAnUnwoundListElement) {
    expectValueCounts("UNWIND [1, 1, 2] AS x RETURN x, count(DISTINCT x)", ValueCountRows {{1, 1}, {2, 1}});
}

// MATCH and UNWIND share no variable, so their rows meet in a cross product: every node
// sees the whole list, charging the two values it carries.
TEST_F(CountDistinctGroupingTest, countsDistinctOfAnUnwoundListPerNode) {
    NameCountRows expected;
    for (const std::string& name : allNodeNames) {
        expected.emplace_back(name, 2u);
    }

    expectNameCounts("MATCH (a) UNWIND [1, 1, 2] AS x RETURN a.name, count(DISTINCT x)", expected);
}

// A chunk size below a group's row count spreads its rows over several chunks. The
// per-group seen-sets are reset with the rest of the accumulator, once at function
// scope, so neither the groups nor the tallies may grow with the number of chunks.
TEST_F(CountDistinctGroupingTest, countsDistinctUnderTwoKeysAcrossChunkBoundaries) {
    NameValueCountRows expected;
    expected.emplace_back(std::optional<std::string>("Remy"), 32, 2u);
    expected.emplace_back(std::optional<std::string>("Adam"), 32, 1u);
    expected.emplace_back(std::optional<std::string>("Ghosts"), std::nullopt, 1u);

    const std::string_view query = "MATCH (a)-->(b)-->(c) RETURN a.name, a.age, count(DISTINCT b)";
    expectNameValueCounts(query, expected, /*chunkSize=*/1);
    expectNameValueCounts(query, expected, /*chunkSize=*/3);
}

// An aggregate has no value to charge until its group is complete, so it cannot be the
// argument of another one - written inline or reached through the alias of an aggregate
// of the same projection.
TEST_F(CountDistinctGroupingTest, rejectsANestedDistinctCount) {
    ScalarCountSink sink;
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN count(DISTINCT count(b))", &sink), TuringException);
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN a.name, count(DISTINCT count(b))", &sink), TuringException);
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN count(b) AS tally, count(DISTINCT tally)", &sink),
                 TuringException);
}
