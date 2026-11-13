#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

// A query of _prefix, N times _clause, then _suffix. A # in the clause stands for its number.
struct ChainQuery {
    std::string _prefix;
    std::string _clause;
    std::string _suffix;
};

}

// Each query repeats a filter on the age of a node read in front of an OPTIONAL MATCH or a CALL, so
// the placement carries every comparison, and every conjunction chaining them, into its body and
// back out. The trim then drops all of them but the last conjunction, from the op, its yield and each op
// between. Four times the clauses must take the trim about four times as long, not the sixteen it
// took when each column dropped rebuilt every op it crossed.
class TrimUnreadColumnsGrowthTest : public CallV3Test {
protected:
    static constexpr size_t SHORT_CHAIN = 500;
    static constexpr size_t LONG_CHAIN = 2000;
    static constexpr size_t TIMED_RUNS = 5;
    static constexpr double LINEAR_GROWTH_BOUND = 8.0;

    struct TrimmedChain {
        mlir::MLIRContext _context;
        mlir::OwningOpRef<mlir::ModuleOp> _module;
        double _seconds {0};
    };

    static void buildQuery(const ChainQuery& chain, size_t length, std::string& query) {
        query = chain._prefix;

        for (size_t index = 0; index < length; index++) {
            std::string numbered = chain._clause;
            const size_t mark = numbered.find('#');
            if (mark != std::string::npos) {
                numbered.replace(mark, 1, std::to_string(index + 1));
            }

            query += numbered;
        }

        query += chain._suffix;
    }

    // The program the trim run after the placement of row-invariant ops starts from
    void programBeforeTheTrim(const ChainQuery& chain, size_t length, std::string& program) {
        std::string query;
        buildQuery(chain, length, query);

        StringRowSink sink;
        runQuery("EXPLAIN (before trim_unread_columns) " + query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before trim_unread_columns 2") {
                program = row.back();
            }
        }

        ASSERT_FALSE(program.empty());
    }

    static void trimProgram(const std::string& program, TrimmedChain& trimmed) {
        trimmed._context.getOrLoadDialect<mlir::func::FuncDialect>();
        trimmed._context.getOrLoadDialect<mlir::storage::Storage>();
        trimmed._context.getOrLoadDialect<mlir::db::DB>();

        trimmed._module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&trimmed._context));
        ASSERT_TRUE(trimmed._module) << program;

        mlir::PassManager passManager(&trimmed._context);
        passManager.enableVerifier(false);
        passManager.addPass(mlir::db::createTrimUnreadColumns());

        const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        ASSERT_TRUE(mlir::succeeded(passManager.run(*trimmed._module)));
        const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        trimmed._seconds = std::chrono::duration<double>(end - start).count();
    }

    static void fastestTrim(const std::string& program, double& seconds) {
        seconds = 0;
        for (size_t run = 0; run < TIMED_RUNS; run++) {
            TrimmedChain trimmed;
            ASSERT_NO_FATAL_FAILURE(trimProgram(program, trimmed));

            seconds = run == 0 ? trimmed._seconds : std::min(seconds, trimmed._seconds);
        }
    }

    void expectLinearGrowth(const ChainQuery& chain) {
        std::string shortProgram;
        std::string longProgram;
        ASSERT_NO_FATAL_FAILURE(programBeforeTheTrim(chain, SHORT_CHAIN, shortProgram));
        ASSERT_NO_FATAL_FAILURE(programBeforeTheTrim(chain, LONG_CHAIN, longProgram));

        double shortSeconds = 0;
        double longSeconds = 0;
        ASSERT_NO_FATAL_FAILURE(fastestTrim(shortProgram, shortSeconds));
        ASSERT_NO_FATAL_FAILURE(fastestTrim(longProgram, longSeconds));

        EXPECT_LT(longSeconds, LINEAR_GROWTH_BOUND * shortSeconds)
            << SHORT_CHAIN << " clauses: " << shortSeconds << " s, " << LONG_CHAIN << " clauses: " << longSeconds << " s";
    }

    void trimChain(const ChainQuery& chain, TrimmedChain& trimmed) {
        std::string program;
        ASSERT_NO_FATAL_FAILURE(programBeforeTheTrim(chain, SHORT_CHAIN, program));
        ASSERT_NO_FATAL_FAILURE(trimProgram(program, trimmed));
    }

    // How many columns each @param CarryingOp carries in, in the order the ops come
    template <typename CarryingOp>
    static void collectCarriedCounts(mlir::ModuleOp module, std::vector<size_t>& counts) {
        module->walk<mlir::WalkOrder::PreOrder>([&counts](CarryingOp op) {
            counts.push_back(op->getNumOperands());
        });
    }

    void expectRows(const ChainQuery& chain, const Rows& expected) {
        std::string query;
        buildQuery(chain, LONG_CHAIN, query);

        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected);
    }
};

// Remy and Adam are the people with an age, which every filter keeps, and 7 edges leave them
TEST_F(TrimUnreadColumnsGrowthTest, dropsWhatAnOptionalMatchHandsBackAndNothingReads) {
    const ChainQuery chain {"MATCH (s:Person) OPTIONAL MATCH (s)-->(b) WITH s, b", " WITH s, b WHERE s.age > -#", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    TrimmedChain trimmed;
    ASSERT_NO_FATAL_FAILURE(trimChain(chain, trimmed));

    std::vector<size_t> carriedCounts;
    collectCarriedCounts<mlir::db::OptionalMatch>(*trimmed._module, carriedCounts);
    EXPECT_EQ(carriedCounts, std::vector<size_t> {3});

    expectRows(chain, {{"7"}});
}

// The comparisons cross the CALL, the OPTIONAL MATCH in its body and both hops in that one. The
// 8 paths of two edges leaving Remy or Adam each count their first node.
TEST_F(TrimUnreadColumnsGrowthTest, dropsWhatACallHandsBackThroughAnOptionalMatchAndNothingReads) {
    const ChainQuery chain {"MATCH (s:Person) CALL { WITH s OPTIONAL MATCH (s)-->(b)-->(c) RETURN b } WITH s, b",
                            " WITH s, b WHERE s.age > -#",
                            " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    TrimmedChain trimmed;
    ASSERT_NO_FATAL_FAILURE(trimChain(chain, trimmed));

    std::vector<size_t> callCarriedCounts;
    collectCarriedCounts<mlir::db::CallSubquery>(*trimmed._module, callCarriedCounts);
    EXPECT_EQ(callCarriedCounts, std::vector<size_t> {3});

    std::vector<size_t> optionalCarriedCounts;
    collectCarriedCounts<mlir::db::OptionalMatch>(*trimmed._module, optionalCarriedCounts);
    EXPECT_EQ(optionalCarriedCounts, std::vector<size_t> {3});

    expectRows(chain, {{"8"}});
}
