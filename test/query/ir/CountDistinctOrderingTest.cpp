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
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Counts = std::vector<uint64_t>;
using Names = std::vector<std::optional<std::string>>;
using NameCountRow = std::pair<std::optional<std::string>, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;
using AgeCountRow = std::pair<std::optional<int64_t>, uint64_t>;
using AgeCountRows = std::vector<AgeCountRow>;

// The nine simpledb sources of an out-edge, by name ascending, each with the number of
// distinct nodes it points at. Remy reaches Adam, Ghosts, Computers and Eighties; Adam
// reaches Remy, Bio and Cooking; Cyrus, Luc, Maxime and Suhas two each; Doruk, Ghosts and
// Martina one each.
const NameCountRows groupsByNameAscending = {
    {"Adam", 3},
    {"Cyrus", 2},
    {"Doruk", 1},
    {"Ghosts", 1},
    {"Luc", 2},
    {"Martina", 1},
    {"Maxime", 2},
    {"Remy", 4},
    {"Suhas", 2},
};

// The same nine groups by distinct count descending, ties broken by name ascending - the
// order the compound key asks for, and one no single key produces.
const NameCountRows groupsByCountDescendingThenName = {
    {"Remy", 4},
    {"Adam", 3},
    {"Cyrus", 2},
    {"Luc", 2},
    {"Maxime", 2},
    {"Suhas", 2},
    {"Doruk", 1},
    {"Ghosts", 1},
    {"Martina", 1},
};

// The four simpledb sources whose hasPhD is true - Remy, Adam, Luc and Martina - by
// distinct target count ascending. Their four counts are all different, so the count
// alone is a total order over these groups and a test of it needs no tie breaker.
const NameCountRows doctorGroupsByCountAscending = {
    {"Martina", 1},
    {"Luc", 2},
    {"Adam", 3},
    {"Remy", 4},
};

const std::string_view doctorGroupsMatch = "MATCH (a)-->(b) WHERE a.hasPhD = true ";

// Collects the (name, count) rows in the order the sink sees them. An ORDER BY is only
// correct if that order survives to the output, so nothing here sorts what it collected.
class OrderedNameCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);

        _appendCalls++;

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

    const NameCountRows& rows() const { return _rows; }

    // How many chunks the emit was cut into, so a test can tell it really saw several
    size_t getAppendCalls() const { return _appendCalls; }

private:
    NameCountRows _rows;
    size_t _appendCalls {0};
};

// The int64-keyed sibling, for a grouping key that is a numeric property rather than a
// name.
class OrderedAgeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ages = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(ages, nullptr);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);

        const auto& ageRaw = ages->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(ageRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    const AgeCountRows& rows() const { return _rows; }

private:
    AgeCountRows _rows;
};

// Collects the single ui64 column a scalar distinct count emits, in emitted order, so a
// row cut over it is read as the number of rows it left rather than as a value.
class OrderedCountSink : public NLOutputSink {
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

// Collects the single name column of a projection that returns the grouping key alone and
// orders on a distinct count it does not carry.
class OrderedNameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _values.push_back(name);
        }
    }

    const Names& values() const { return _values; }

private:
    Names _values;
};

}

// How a count(DISTINCT x) projection meets the row-reordering and row-cutting clauses:
// ORDER BY over the grouping key, over the distinct count itself and over expressions on
// either, SKIP and LIMIT over the groups and over the one row of a scalar distinct count,
// and a DISTINCT projection on top of an aggregating one. The distinct count is the same
// non-nullable ui64 column a plain grouped count is, so every op that carries a plain
// count has to carry this one.
class CountDistinctOrderingTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
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

        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, sink, &memory, chunkSize);
        interpreter.run();
    }

    void expectRows(std::string_view query,
                    const NameCountRows& expected,
                    size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        OrderedNameCountSink sink;
        runQuery(query, &sink, chunkSize);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectAgeRows(std::string_view query, const AgeCountRows& expected) {
        OrderedAgeCountSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        OrderedCountSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), expected) << "query: " << query;
    }

    void expectNames(std::string_view query, const Names& expected) {
        OrderedNameSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), expected) << "query: " << query;
    }

    // @param tail is appended to the match the four hasPhD groups come from, so a test
    // over them spells out only the clauses it is about
    void expectDoctorRows(std::string_view tail, const NameCountRows& expected) {
        const std::string query = std::string(doctorGroupsMatch) + std::string(tail);

        expectRows(query, expected);
    }

    // The rows of @param expected from @param first, as many as @param count - the window
    // a SKIP and a LIMIT cut out of an order
    void windowOf(const NameCountRows& expected, size_t first, size_t count, NameCountRows& rows) {
        rows.assign(expected.begin() + first, expected.begin() + first + count);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKey) {
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name", groupsByNameAscending);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKeyDescending) {
    const NameCountRows expected {groupsByNameAscending.rbegin(), groupsByNameAscending.rend()};

    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name DESC", expected);
}

// An alias is another spelling of the item it names, on the grouping key and on the
// distinct count alike, so aliasing both changes neither the order nor the counts.
TEST_F(CountDistinctOrderingTest, ordersGroupsByTheAliasOfTheGroupingKey) {
    expectRows("MATCH (a)-->(b) RETURN a.name AS sourceName, count(DISTINCT b) AS targets ORDER BY sourceName",
               groupsByNameAscending);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheAliasOfTheGroupingKeyDescending) {
    const NameCountRows expected {groupsByNameAscending.rbegin(), groupsByNameAscending.rend()};

    expectRows("MATCH (a)-->(b) RETURN a.name AS sourceName, count(DISTINCT b) AS targets "
               "ORDER BY sourceName DESC",
               expected);
}

// The grouping key is a.age, which only Remy and Adam carry: their two groups fold into
// one charging the seven nodes they reach between them, and the seven ageless sources
// make the null group, which reaches nine. A null sorts after every value, so it closes
// the ascending order. The key computes over the grouped column rather than over the one
// value per matched row the grouping consumed.
TEST_F(CountDistinctOrderingTest, ordersGroupsByAnExpressionOverTheGroupingKey) {
    const AgeCountRows expected {{32, 7}, {std::nullopt, 9}};

    expectAgeRows("MATCH (a)-->(b) RETURN a.age, count(DISTINCT b) ORDER BY a.age + 1", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByAnExpressionOverTheGroupingKeyDescending) {
    const AgeCountRows expected {{std::nullopt, 9}, {32, 7}};

    expectAgeRows("MATCH (a)-->(b) RETURN a.age, count(DISTINCT b) ORDER BY a.age + 1 DESC", expected);
}

// A bounded sort keeps only the best k groups, so the distinct count is carried by the
// trim that drops the rest as well as by the emit.
TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKeyThenLimits) {
    NameCountRows expected;
    windowOf(groupsByNameAscending, 0, 3, expected);

    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name LIMIT 3", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKeyDescendingThenLimits) {
    const NameCountRows expected {groupsByNameAscending.rbegin(), groupsByNameAscending.rbegin() + 3};

    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name DESC LIMIT 3", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKeyThenSkipsAndLimits) {
    NameCountRows expected;
    windowOf(groupsByNameAscending, 2, 3, expected);

    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name SKIP 2 LIMIT 3", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheGroupingKeyThenSkips) {
    NameCountRows expected;
    windowOf(groupsByNameAscending, 2, groupsByNameAscending.size() - 2, expected);

    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name SKIP 2", expected);
}

// A limit of more rows than there are groups keeps them all, and a limit of none keeps
// none: the cut is charged to the group rows and not to the one seen-set behind them.
TEST_F(CountDistinctOrderingTest, cutsTheGroupsPastTheirNumber) {
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name LIMIT 20",
               groupsByNameAscending);
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name LIMIT 0", {});
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name SKIP 9", {});
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name SKIP 20", {});
}

// Without an ORDER BY the groups come back in the order they were grouped in, which is
// not a guarantee of the language: only how many rows the cut leaves, and that each is a
// group of the projection.
TEST_F(CountDistinctOrderingTest, skipsAndLimitsGroupsWithoutAnOrder) {
    OrderedNameCountSink sink;
    runQuery("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) SKIP 1 LIMIT 2", &sink);

    const NameCountRows& rows = sink.rows();
    ASSERT_EQ(rows.size(), 2u);

    for (const NameCountRow& row : rows) {
        const auto findIt = std::find(groupsByNameAscending.begin(), groupsByNameAscending.end(), row);
        EXPECT_NE(findIt, groupsByNameAscending.end()) << "unexpected row: " << row.second;
    }
}

// A scalar distinct count is one row, so a limit keeps it or cuts it and any skip at all
// leaves nothing behind. The eighteen out-edges reach twelve distinct nodes.
TEST_F(CountDistinctOrderingTest, cutsTheOneRowOfAScalarDistinctCount) {
    expectCounts("MATCH (a)-->(b) RETURN count(DISTINCT b) LIMIT 1", Counts {12});
    expectCounts("MATCH (a)-->(b) RETURN count(DISTINCT b) LIMIT 20", Counts {12});
    expectCounts("MATCH (a)-->(b) RETURN count(DISTINCT b) LIMIT 0", Counts {});
    expectCounts("MATCH (a)-->(b) RETURN count(DISTINCT b) SKIP 1", Counts {});
    expectCounts("MATCH (a)-->(b) RETURN count(DISTINCT b) SKIP 1 LIMIT 1", Counts {});
}

// An aggregating projection already emits one row per group, and its grouping keys are
// distinct by construction, so a DISTINCT on top of it drops nothing: the same groups in
// the same order, and the same distinct counts.
TEST_F(CountDistinctOrderingTest, keepsEveryGroupUnderADistinctProjection) {
    expectRows("MATCH (a)-->(b) RETURN DISTINCT a.name, count(DISTINCT b) ORDER BY a.name",
               groupsByNameAscending);
    expectCounts("MATCH (a)-->(b) RETURN DISTINCT count(DISTINCT b)", Counts {12});
}

TEST_F(CountDistinctOrderingTest, cutsTheGroupsUnderADistinctProjection) {
    NameCountRows expected;
    windowOf(groupsByNameAscending, 2, 3, expected);

    expectRows("MATCH (a)-->(b) RETURN DISTINCT a.name, count(DISTINCT b) ORDER BY a.name SKIP 2 LIMIT 3",
               expected);
    expectCounts("MATCH (a)-->(b) RETURN DISTINCT count(DISTINCT b) LIMIT 0", Counts {});
}

// The same order over a chunk of two rows: the nine groups reach the sort in five steps,
// so the distinct count is appended onto its buffer once per step and gathered back out
// in permutation order across the chunk boundaries.
TEST_F(CountDistinctOrderingTest, ordersGroupsAcrossSeveralChunks) {
    OrderedNameCountSink sink;
    runQuery("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) ORDER BY a.name", &sink, /*chunkSize=*/2);

    EXPECT_GT(sink.getAppendCalls(), 1u);
    EXPECT_EQ(sink.rows(), groupsByNameAscending);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheDistinctCount) {
    expectDoctorRows("RETURN a.name, count(DISTINCT b) ORDER BY count(DISTINCT b)",
                     doctorGroupsByCountAscending);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheDistinctCountDescending) {
    const NameCountRows expected {doctorGroupsByCountAscending.rbegin(), doctorGroupsByCountAscending.rend()};

    expectDoctorRows("RETURN a.name, count(DISTINCT b) ORDER BY count(DISTINCT b) DESC", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheAliasOfTheDistinctCount) {
    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets",
                     doctorGroupsByCountAscending);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheAliasOfTheDistinctCountDescending) {
    const NameCountRows expected {doctorGroupsByCountAscending.rbegin(), doctorGroupsByCountAscending.rend()};

    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets DESC", expected);
}

// The top-N shape: the groups with the most distinct targets, and only those. A bounded
// sort keyed on the distinct count has to trim on that column, not only emit it.
TEST_F(CountDistinctOrderingTest, ordersGroupsByTheDistinctCountDescendingThenLimits) {
    const NameCountRows expected {doctorGroupsByCountAscending.rbegin(),
                                  doctorGroupsByCountAscending.rbegin() + 2};

    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets DESC LIMIT 2", expected);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByTheDistinctCountThenSkipsAndLimits) {
    NameCountRows expected;
    windowOf(doctorGroupsByCountAscending, 1, 2, expected);

    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets SKIP 1 LIMIT 2", expected);
}

// Six of the nine groups share a distinct count with another, so the count alone leaves
// them tied and the grouping key beside it decides: one key read off the count column and
// one off the grouped key column, both holding the same row per group.
TEST_F(CountDistinctOrderingTest, breaksTiesOfTheDistinctCountWithTheGroupingKey) {
    expectRows("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets DESC, a.name",
               groupsByCountDescendingThenName);
}

TEST_F(CountDistinctOrderingTest, ordersGroupsByAnExpressionOverTheDistinctCount) {
    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY targets + 1",
                     doctorGroupsByCountAscending);
}

// Adding one to every count changes no order, so the case above would also pass on a sort
// that ignored the key and used the count column beside it. Negating does reorder the
// groups, so this is the case that pins the key as the thing being evaluated.
TEST_F(CountDistinctOrderingTest, ordersGroupsByANegationOfTheDistinctCount) {
    const NameCountRows expected {doctorGroupsByCountAscending.rbegin(), doctorGroupsByCountAscending.rend()};

    expectDoctorRows("RETURN a.name, count(DISTINCT b) AS targets ORDER BY 0 - targets", expected);
}

// The distinct count is the key and nothing else: it is computed for the sort and left out
// of the output, the way an unprojected key over a grouping key already is.
TEST_F(CountDistinctOrderingTest, ordersGroupsByADistinctCountItDoesNotProject) {
    const Names expected {"Remy", "Adam", "Luc", "Martina"};
    const std::string query = std::string(doctorGroupsMatch) + "RETURN a.name ORDER BY count(DISTINCT b) DESC";

    expectNames(query, expected);
}

TEST_F(CountDistinctOrderingTest, ordersDistinctGroupsByTheDistinctCount) {
    const NameCountRows expected {doctorGroupsByCountAscending.rbegin(), doctorGroupsByCountAscending.rend()};

    expectDoctorRows("RETURN DISTINCT a.name, count(DISTINCT b) AS targets ORDER BY targets DESC", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
