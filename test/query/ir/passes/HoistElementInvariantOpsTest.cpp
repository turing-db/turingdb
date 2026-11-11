#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "QueryStatus.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The body of an op over the elements of a list runs once per element. What it computes
// from the columns it carries in alone, reaching no element, accumulator or row tag, holds
// one value per row, so it is computed once per row in front of the op and carried in.
// Remy and Adam are the two people aged 32; no other person has an age.
class HoistElementInvariantOpsTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5, but none where i % 11 = 0.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    struct OpCount {
        size_t _inElementBodies {0};
        size_t _total {0};
    };

    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    static bool isInElementBody(mlir::Operation* op) {
        for (mlir::Operation* parent = op->getParentOp(); parent; parent = parent->getParentOp()) {
            if (mlir::isa<mlir::db::ListComprehension, mlir::db::ListPredicate, mlir::db::Reduce, mlir::db::AllReduce>(parent)) {
                return true;
            }
        }

        return false;
    }

    void countOps(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> counted, OpCount& count, std::string& program) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        module.walk([counted, &count](mlir::Operation* op) {
            if (!counted(op)) {
                return;
            }

            count._total++;
            if (isInElementBody(op)) {
                count._inElementBodies++;
            }
        });

        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    void expectNodePropertyReads(std::string_view query, llvm::StringRef property, size_t inElementBodies, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            return read && read.getProperty() == property;
        };

        OpCount count;
        std::string program;
        countOps(query, readsTheProperty, count, program);

        EXPECT_EQ(count._inElementBodies, inElementBodies) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    template <typename CountedOp>
    void expectOps(std::string_view query, size_t inElementBodies, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        OpCount count;
        std::string program;
        countOps(query, isCounted, count, program);

        EXPECT_EQ(count._inElementBodies, inElementBodies) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (:Item {i: i, v: CASE WHEN i % 11 <> 0 THEN i % 5 END})");
    }

    static bool hasValue(int64_t item) {
        return item % 11 != 0;
    }

    // Prints the first row that differs rather than every row
    void expectManyRows(std::string_view query, Rows expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), expected.begin());
        if (actualIt == rows.end()) {
            return;
        }

        std::string actualText;
        describeRows(Rows {*actualIt}, actualText);

        std::string expectedText;
        describeRows(Rows {*expectedIt}, expectedText);

        ADD_FAILURE() << "query: " << query << "\nfirst differing row: " << actualText << "expected: " << expectedText;
    }

private:
    mlir::MLIRContext _context;
};

// The name, used after the WHERE alone, is read for each element it keeps
TEST_F(HoistElementInvariantOpsTest, readsTheOuterPropertiesOfAComprehensionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN [x IN [1, 2] WHERE x > n.age | n.name]";
    expectNodePropertyReads(query, "age", 0, 1);
    expectNodePropertyReads(query, "name", 1, 1);

    expectRows("MATCH (n:Person) RETURN [x IN [30, 40] WHERE x > n.age | n.name]",
               {{"[Remy]"}, {"[Adam]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}});
}

TEST_F(HoistElementInvariantOpsTest, readsTheOuterPropertyOfAListPredicateOncePerRow) {
    expectNodePropertyReads("MATCH (n:Person) RETURN any(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN all(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN none(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN single(x IN [1, 2] WHERE x > n.age)", "age", 0, 1);

    const Rows unknownForTheOthers {{"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                                    {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}};

    Rows anyRows {{"Remy", "true"}, {"Adam", "true"}};
    anyRows.insert(anyRows.end(), unknownForTheOthers.begin(), unknownForTheOthers.end());
    expectRows("MATCH (n:Person) RETURN n.name, any(x IN [30, 40] WHERE x > n.age)", anyRows);

    Rows allRows {{"Remy", "false"}, {"Adam", "false"}};
    allRows.insert(allRows.end(), unknownForTheOthers.begin(), unknownForTheOthers.end());
    expectRows("MATCH (n:Person) RETURN n.name, all(x IN [30, 40] WHERE x > n.age)", allRows);

    Rows noneRows {{"Remy", "false"}, {"Adam", "false"}};
    noneRows.insert(noneRows.end(), unknownForTheOthers.begin(), unknownForTheOthers.end());
    expectRows("MATCH (n:Person) RETURN n.name, none(x IN [30, 40] WHERE x > n.age)", noneRows);

    Rows singleRows {{"Remy", "true"}, {"Adam", "true"}};
    singleRows.insert(singleRows.end(), unknownForTheOthers.begin(), unknownForTheOthers.end());
    expectRows("MATCH (n:Person) RETURN n.name, single(x IN [30, 40] WHERE x > n.age)", singleRows);
}

TEST_F(HoistElementInvariantOpsTest, foldsAnOuterPropertyReadOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, reduce(s = 0, x IN [1, 2] | s + x + n.age)";
    expectNodePropertyReads(query, "age", 0, 1);

    expectRows(query, {{"Remy", "67"}, {"Adam", "67"}, {"Maxime", "null"}, {"Luc", "null"},
                       {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

// 0 + 1 + 32 = 33 is below 2 * 32 = 64 and 33 + 2 + 32 = 67 is not
TEST_F(HoistElementInvariantOpsTest, decidesAnAllReduceOverAnOuterPropertyReadOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "allReduce(s = 0, x IN [1, 2] | s + x + n.age, s < 2 * n.age)";
    expectNodePropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::MulOp>(query, 0, 1);

    expectRows(query, {{"Remy", "false"}, {"Adam", "false"}, {"Maxime", "null"}, {"Luc", "null"},
                       {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(HoistElementInvariantOpsTest, computesAnOuterExpressionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | x + n.age * 2]";
    expectNodePropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::MulOp>(query, 0, 1);
    expectOps<mlir::db::AddOp>(query, 1, 1);

    expectRows(query, {{"Remy", "[65, 66]"}, {"Adam", "[65, 66]"}, {"Maxime", "[null, null]"},
                       {"Luc", "[null, null]"}, {"Martina", "[null, null]"}, {"Suhas", "[null, null]"},
                       {"Cyrus", "[null, null]"}, {"Doruk", "[null, null]"}});
}

// Remy and Adam are the founders
TEST_F(HoistElementInvariantOpsTest, testsAnOuterLabelOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE n:Founder | x]";
    expectOps<mlir::db::GetNodeLabelSet>(query, 0, 1);
    expectOps<mlir::db::CheckLabelConstraint>(query, 0, 1);

    expectRows(query, {{"Remy", "[1, 2]"}, {"Adam", "[1, 2]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                       {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(HoistElementInvariantOpsTest, buildsAnInnerComprehensionReadingNoOuterElementOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | [y IN [3] | n.age]]";
    expectNodePropertyReads(query, "age", 0, 1);
    expectOps<mlir::db::ListComprehension>(query, 0, 2);

    expectRows(query, {{"Remy", "[[32], [32]]"}, {"Adam", "[[32], [32]]"}, {"Maxime", "[[null], [null]]"},
                       {"Luc", "[[null], [null]]"}, {"Martina", "[[null], [null]]"},
                       {"Suhas", "[[null], [null]]"}, {"Cyrus", "[[null], [null]]"},
                       {"Doruk", "[[null], [null]]"}});
}

TEST_F(HoistElementInvariantOpsTest, keepsAReadOfTheElementInTheBody) {
    const std::string_view query = "MATCH (n:Person) RETURN [m IN [n] | m.name]";
    expectNodePropertyReads(query, "name", 1, 1);

    expectRows(query, {{"[Remy]"}, {"[Adam]"}, {"[Maxime]"}, {"[Luc]"}, {"[Martina]"}, {"[Suhas]"},
                       {"[Cyrus]"}, {"[Doruk]"}});
}

// Remy and Adam would divide by zero. The division runs only on the elements the WHERE keeps,
// and so does the read of the age it alone uses.
TEST_F(HoistElementInvariantOpsTest, keepsADivisionThatMayRaiseInTheBody) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE x > 5 | 10 / (n.age - 32)]";
    expectNodePropertyReads(query, "age", 1, 1);
    expectOps<mlir::db::DivOp>(query, 1, 1);

    expectRows(query, {{"Remy", "[]"}, {"Adam", "[]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                       {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});

    expectError("MATCH (n:Person) RETURN n.name, [x IN [1, 2] WHERE x > 1 | 10 / (n.age - 32)]",
                "Attempted to divide by zero");
}

TEST_F(HoistElementInvariantOpsTest, carriesTheValuesOfEachRowAcrossChunks) {
    createItems();

    Rows comprehensions;
    Rows sums;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const std::string itemText = std::to_string(item);
        if (!hasValue(item)) {
            comprehensions.push_back({itemText, "[]"});
            sums.push_back({itemText, "null"});
            continue;
        }

        const int64_t value = item % 5;

        std::string list = "[";
        for (int64_t x = value + 1; x <= 3; x++) {
            list += x == value + 1 ? "" : ", ";
            list += std::to_string(item * 10 + x);
        }

        comprehensions.push_back({itemText, list + "]"});
        sums.push_back({itemText, std::to_string(6 * value)});
    }

    expectManyRows("MATCH (n:Item) RETURN n.i, [x IN [1, 2, 3] WHERE x > n.v | n.i * 10 + x]", comprehensions);
    expectManyRows("MATCH (n:Item) RETURN n.i, reduce(s = 0, x IN [1, 2, 3] | s + x * n.v)", sums);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
