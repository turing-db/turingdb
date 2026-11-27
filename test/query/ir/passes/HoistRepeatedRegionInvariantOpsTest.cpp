#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringException.h"
#include "iterators/ChunkConfig.h"

using namespace db;
using namespace turing::test;

// A region run once per element of a list, or once per hop of a walk, repeats the row each
// column it carries in comes from. What it computes from those columns alone holds one value
// per row, so it is computed once per row in front of the op. Remy and Adam are the two
// people aged 32; no other person has an age.
class HoistRepeatedRegionInvariantOpsTest : public HashJoinQueryTest {
protected:
    struct OpCount {
        size_t _inRepeatedRegions {0};
        size_t _total {0};
    };

    static bool isInRepeatedRegion(mlir::Operation* op) {
        for (mlir::Operation* parent = op->getParentOp(); parent; parent = parent->getParentOp()) {
            const bool repeatsRows = mlir::isa<mlir::db::ListComprehension,
                                               mlir::db::ListPredicate,
                                               mlir::db::Reduce,
                                               mlir::db::AllReduce,
                                               mlir::db::ExplorePaths>(parent);
            if (repeatsRows) {
                return true;
            }
        }

        return false;
    }

    void countOps(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> isCounted, OpCount& count, std::string& program) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        module->walk([isCounted, &count](mlir::Operation* op) {
            if (!isCounted(op)) {
                return;
            }

            count._total++;
            if (isInRepeatedRegion(op)) {
                count._inRepeatedRegions++;
            }
        });

        render(module.get(), program);
    }

    void expectPropertyReads(std::string_view query, llvm::StringRef property, size_t inRepeatedRegions, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            if (mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op)) {
                return read.getProperty() == property;
            } else if (mlir::db::GetEdgeProperties read = mlir::dyn_cast<mlir::db::GetEdgeProperties>(op)) {
                return read.getProperty() == property;
            }

            return false;
        };

        OpCount count;
        std::string program;
        countOps(query, readsTheProperty, count, program);

        EXPECT_EQ(count._inRepeatedRegions, inRepeatedRegions) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    template <typename CountedOp>
    void expectOps(std::string_view query, size_t inRepeatedRegions, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        OpCount count;
        std::string program;
        countOps(query, isCounted, count, program);

        EXPECT_EQ(count._inRepeatedRegions, inRepeatedRegions) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    // Prints the first row that differs rather than every row
    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        ASSERT_EQ(rows.size(), sortedExpected.size()) << "query: " << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), sortedExpected.begin());
        if (actualIt != rows.end()) {
            EXPECT_EQ(*actualIt, *expectedIt) << "query: " << query;
        }
    }

    void expectRaises(std::string_view query, std::string_view message) {
        try {
            StringRowSink sink;
            runQuery(query, sink);
        } catch (const TuringException& error) {
            const std::string raised = error.what();
            EXPECT_TRUE(contains(raised, message)) << "query: " << query << "\nerror: " << raised;
            return;
        }

        ADD_FAILURE() << "query raised nothing: " << query;
    }
};

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertiesOfAComprehensionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN [x IN [1, 2] WHERE x > n.age | n.name]";
    expectPropertyReads(query, "age", 0, 1);
    expectPropertyReads(query, "name", 0, 1);

    expectSortedRows("MATCH (n:Person) RETURN n.name, [x IN [30, 40] WHERE x > n.age | n.name]",
                     {{"Remy", "Remy"}, {"Adam", "Adam"}, {"Maxime", ""}, {"Luc", ""},
                      {"Martina", ""}, {"Suhas", ""}, {"Cyrus", ""}, {"Doruk", ""}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertyOfAListPredicateOncePerRow) {
    expectPropertyReads("MATCH (n:Person) RETURN any(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectPropertyReads("MATCH (n:Person) RETURN all(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectPropertyReads("MATCH (n:Person) RETURN none(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectPropertyReads("MATCH (n:Person) RETURN single(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);

    const Rows unknown {{"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                        {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}};

    Rows holds {{"Remy", "true"}, {"Adam", "true"}};
    holds.insert(holds.end(), unknown.begin(), unknown.end());

    Rows fails {{"Remy", "false"}, {"Adam", "false"}};
    fails.insert(fails.end(), unknown.begin(), unknown.end());

    expectSortedRows("MATCH (n:Person) RETURN n.name, any(x IN [30, 40] WHERE x > n.age)", holds);
    expectSortedRows("MATCH (n:Person) RETURN n.name, all(x IN [30, 40] WHERE x > n.age)", fails);
    expectSortedRows("MATCH (n:Person) RETURN n.name, none(x IN [30, 40] WHERE x > n.age)", fails);
    expectSortedRows("MATCH (n:Person) RETURN n.name, single(x IN [30, 40] WHERE x > n.age)", holds);
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, foldsAnOuterPropertyReadOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, reduce(s = 0, x IN [1, 2] | s + x + n.age)";
    expectPropertyReads(query, "age", 0, 1);

    expectSortedRows(query, {{"Remy", "67"}, {"Adam", "67"}, {"Maxime", "null"}, {"Luc", "null"},
                             {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

// 0 + 1 + 32 = 33 is below 2 * 32 = 64, and 33 + 2 + 32 = 67 is not
TEST_F(HoistRepeatedRegionInvariantOpsTest, decidesAnAllReduceOverAnOuterPropertyOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "allReduce(s = 0, x IN [1, 2] | s + x + n.age, s < 2 * n.age)";
    expectPropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::MulOp>(query, 0, 1);

    expectSortedRows(query, {{"Remy", "false"}, {"Adam", "false"}, {"Maxime", "null"}, {"Luc", "null"},
                             {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, computesAnOuterExpressionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | x + n.age * 2]";
    expectPropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::MulOp>(query, 0, 1);
    expectOps<mlir::db::AddOp>(query, 1, 1);

    expectSortedRows(query, {{"Remy", "65, 66"}, {"Adam", "65, 66"}, {"Maxime", "null, null"},
                             {"Luc", "null, null"}, {"Martina", "null, null"}, {"Suhas", "null, null"},
                             {"Cyrus", "null, null"}, {"Doruk", "null, null"}});
}

// Remy and Adam are the founders
TEST_F(HoistRepeatedRegionInvariantOpsTest, testsAnOuterLabelOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE n:Founder | x]";
    expectOps<mlir::db::GetNodeLabelSet>(query, 0, 1);
    expectOps<mlir::db::CheckLabelConstraint>(query, 0, 1);

    expectSortedRows(query, {{"Remy", "1, 2"}, {"Adam", "1, 2"}, {"Maxime", ""}, {"Luc", ""},
                             {"Martina", ""}, {"Suhas", ""}, {"Cyrus", ""}, {"Doruk", ""}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertyOfAnInnerComprehensionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | [y IN [3] | n.age]]";
    expectPropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::ListComprehension>(query, 1, 2);

    expectSortedRows(query, {{"Remy", "[32], [32]"}, {"Adam", "[32], [32]"}, {"Maxime", "[null], [null]"},
                             {"Luc", "[null], [null]"}, {"Martina", "[null], [null]"},
                             {"Suhas", "[null], [null]"}, {"Cyrus", "[null], [null]"},
                             {"Doruk", "[null], [null]"}});
}

// The first row's list is empty, so the body runs no time for it. In front of the outer
// comprehension, the op with a region would run for that row too.
TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsAnOpWithARegionInTheBody) {
    const std::string_view comprehension = "UNWIND [[], [1, 2]] AS xs RETURN [x IN xs | [y IN range(1, 3) | size(xs)]] AS v";
    expectOps<mlir::db::ListComprehension>(comprehension, 1, 2);
    expectSortedRows(comprehension, {{""}, {"[2, 2, 2], [2, 2, 2]"}});

    const std::string_view predicate = "UNWIND [[], [1, 2]] AS xs RETURN [x IN xs | any(y IN range(1, 3) WHERE y < size(xs))] AS v";
    expectOps<mlir::db::ListPredicate>(predicate, 1, 1);
    expectSortedRows(predicate, {{""}, {"true, true"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsAReadOfTheElementInTheBody) {
    const std::string_view query = "MATCH (n:Person) RETURN [m IN [n] | m.name]";
    expectPropertyReads(query, "name", 1, 1);

    expectSortedRows(query, {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"},
                             {"Cyrus"}, {"Doruk"}});
}

// Remy and Adam would divide by zero. The age is read once per row, but the division runs
// only on the elements the WHERE keeps.
TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsADivisionThatMayRaiseInTheBody) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE x > 5 | 10 / (n.age - 32)]";
    expectPropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::DivOp>(query, 1, 1);

    expectSortedRows(query, {{"Remy", ""}, {"Adam", ""}, {"Maxime", ""}, {"Luc", ""},
                             {"Martina", ""}, {"Suhas", ""}, {"Cyrus", ""}, {"Doruk", ""}});

    expectRaises("MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE x > 1 | 10 / (n.age - 32)]",
                 "Attempted to divide by zero");
}

// Every hop leaving Remy or Adam but the ones to Computers, Bio and Cooking, which have no
// duration, and the one from Ghosts, which lasts 200, lasts less than 32
TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheSeedPropertyOfAWalkOncePerRow) {
    const Rows reached {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Eighties"},
                        {"Remy", "Remy"}, {"Remy", "Ghosts"}, {"Remy", "Eighties"},
                        {"Adam", "Remy"}, {"Adam", "Adam"}, {"Adam", "Ghosts"}, {"Adam", "Eighties"}};

    const std::string_view quantified = "MATCH (n:Person)(()-[e]->() WHERE e.duration < n.age){1,3}(m) "
                                        "RETURN n.name, m.name";
    expectPropertyReads(quantified, "age", 0, 1);
    expectSortedRows(quantified, reached);

    const std::string_view listPredicate = "MATCH p = (n:Person)-[*1..3]->(m) "
                                           "WHERE all(r IN relationships(p) WHERE r.duration < n.age) "
                                           "RETURN n.name, m.name";
    expectPropertyReads(listPredicate, "age", 0, 1);
    expectSortedRows(listPredicate, reached);
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, carriesTheValuesOfEachRowAcrossChunks) {
    const int64_t rowCount = 2 * static_cast<int64_t>(ChunkConfig::CHUNK_SIZE) + 7;
    const std::string range = "UNWIND range(0, " + std::to_string(rowCount - 1) + ") AS i ";

    const std::string comprehension = range + "RETURN i, [x IN [1, 2, 3] WHERE x > i % 5 | i * 10 + x]";
    const std::string sum = range + "RETURN i, reduce(s = 0, x IN [1, 2, 3] | s + x * (i % 5))";
    expectOps<mlir::db::ModOp>(comprehension, 0, 1);
    expectOps<mlir::db::ModOp>(sum, 0, 1);

    Rows comprehensions;
    Rows sums;
    for (int64_t row = 0; row < rowCount; row++) {
        const std::string rowText = std::to_string(row);
        const int64_t value = row % 5;

        std::string list;
        for (int64_t x = value + 1; x <= 3; x++) {
            list += x == value + 1 ? "" : ", ";
            list += std::to_string(row * 10 + x);
        }

        comprehensions.push_back({rowText, list});
        sums.push_back({rowText, std::to_string(6 * value)});
    }

    expectSortedRows(comprehension, comprehensions);
    expectSortedRows(sum, sums);
}
