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

// A query of clauses repeated N times: _prefix, N times _clause, _suffix, then N times _tail. A #
// in a repeated clause stands for its number.
struct ChainQuery {
    std::string _prefix;
    std::string _clause;
    std::string _suffix;
    std::string _tail;
};

}

// Each query repeats a clause reading columns in front of an op repeating rows - a product, a
// hop, a walk, an OPTIONAL MATCH, a CALL - or used only behind a filter, so one column per
// clause crosses that op. Four times the clauses must take the pass about four times as long,
// not the sixteen it took when each column rebuilt the op.
class PlaceRowInvariantOpsGrowthTest : public CallV3Test {
protected:
    static constexpr size_t SHORT_CHAIN = 500;
    static constexpr size_t LONG_CHAIN = 2000;
    static constexpr size_t TIMED_RUNS = 5;
    static constexpr double LINEAR_GROWTH_BOUND = 8.0;

    struct PlacedChain {
        mlir::MLIRContext _context;
        mlir::OwningOpRef<mlir::ModuleOp> _module;
        double _seconds {0};
    };

    static void buildQuery(const ChainQuery& chain, size_t length, std::string& query) {
        query = chain._prefix;
        appendClauses(chain._clause, length, query);
        query += chain._suffix;
        appendClauses(chain._tail, length, query);
    }

    static void appendClauses(const std::string& clause, size_t length, std::string& query) {
        if (clause.empty()) {
            return;
        }

        for (size_t index = 0; index < length; index++) {
            std::string numbered = clause;
            const size_t mark = numbered.find('#');
            if (mark != std::string::npos) {
                numbered.replace(mark, 1, std::to_string(index + 1));
            }

            query += numbered;
        }
    }

    void programBeforeThePass(const ChainQuery& chain, size_t length, std::string& program) {
        std::string query;
        buildQuery(chain, length, query);

        StringRowSink sink;
        runQuery("EXPLAIN (before place_row_invariant_ops) " + query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before place_row_invariant_ops") {
                program = row.back();
            }
        }
    }

    static void placeProgram(const std::string& program, PlacedChain& placed) {
        placed._context.getOrLoadDialect<mlir::func::FuncDialect>();
        placed._context.getOrLoadDialect<mlir::storage::Storage>();
        placed._context.getOrLoadDialect<mlir::db::DB>();

        placed._module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&placed._context));
        ASSERT_TRUE(placed._module) << program;

        mlir::PassManager passManager(&placed._context);
        passManager.enableVerifier(false);
        passManager.addPass(mlir::db::createPlaceRowInvariantOps());

        const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        ASSERT_TRUE(mlir::succeeded(passManager.run(*placed._module)));
        const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        placed._seconds = std::chrono::duration<double>(end - start).count();
    }

    static void fastestPlacement(const std::string& program, double& seconds) {
        seconds = 0;
        for (size_t run = 0; run < TIMED_RUNS; run++) {
            PlacedChain placed;
            ASSERT_NO_FATAL_FAILURE(placeProgram(program, placed));

            seconds = run == 0 ? placed._seconds : std::min(seconds, placed._seconds);
        }
    }

    void expectLinearGrowth(const ChainQuery& chain, size_t shortLength = SHORT_CHAIN, size_t longLength = LONG_CHAIN) {
        std::string shortProgram;
        std::string longProgram;
        ASSERT_NO_FATAL_FAILURE(programBeforeThePass(chain, shortLength, shortProgram));
        ASSERT_NO_FATAL_FAILURE(programBeforeThePass(chain, longLength, longProgram));

        double shortSeconds = 0;
        double longSeconds = 0;
        ASSERT_NO_FATAL_FAILURE(fastestPlacement(shortProgram, shortSeconds));
        ASSERT_NO_FATAL_FAILURE(fastestPlacement(longProgram, longSeconds));

        EXPECT_LT(longSeconds, LINEAR_GROWTH_BOUND * shortSeconds)
            << shortLength << " clauses: " << shortSeconds << " s, " << longLength << " clauses: " << longSeconds << " s";
    }

    void placeChain(const ChainQuery& chain, PlacedChain& placed) {
        std::string program;
        ASSERT_NO_FATAL_FAILURE(programBeforeThePass(chain, SHORT_CHAIN, program));
        ASSERT_NO_FATAL_FAILURE(placeProgram(program, placed));
    }

    static bool readsNodeProperty(mlir::Operation* op, llvm::StringRef property) {
        mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
        return read && read.getProperty() == property;
    }

    // How many ops @param isCounted picks sit in a region of a @param BodyOp
    template <typename BodyOp, typename IsCounted>
    static size_t countInside(mlir::ModuleOp module, IsCounted isCounted) {
        size_t count = 0;
        module->walk([&isCounted, &count](mlir::Operation* op) {
            if (isCounted(op) && op->getParentOfType<BodyOp>()) {
                count++;
            }
        });

        return count;
    }

    // How many ops @param isCounted picks come before the first @param MarkerOp
    template <typename MarkerOp, typename IsCounted>
    static size_t countBefore(mlir::ModuleOp module, IsCounted isCounted) {
        size_t count = 0;
        bool markerSeen = false;
        module->walk<mlir::WalkOrder::PreOrder>([&isCounted, &count, &markerSeen](mlir::Operation* op) {
            markerSeen = markerSeen || mlir::isa<MarkerOp>(op);
            if (!markerSeen && isCounted(op)) {
                count++;
            }
        });

        return count;
    }

    template <typename BodyOp>
    static size_t countReadsInside(mlir::ModuleOp module, llvm::StringRef property) {
        return countInside<BodyOp>(module, [property](mlir::Operation* op) {
            return readsNodeProperty(op, property);
        });
    }

    template <typename MarkerOp>
    static size_t countReadsBefore(mlir::ModuleOp module, llvm::StringRef property) {
        return countBefore<MarkerOp>(module, [property](mlir::Operation* op) {
            return readsNodeProperty(op, property);
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

// Adam is 32 and Remy the only other person with an age, so 3 edges end at someone at least as old
TEST_F(PlaceRowInvariantOpsGrowthTest, readsEachSideOfAJoinChainInItsOwnFactor) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'}) MATCH (a)-->(b) WITH s, a, b", " WITH s, a, b WHERE b.age >= s.age", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsInside<mlir::db::CrossProduct>(*placed._module, "age"), 2 * SHORT_CHAIN);

    mlir::db::CrossProduct product;
    placed._module->walk([&product](mlir::db::CrossProduct op) {
        product = op;
    });
    ASSERT_TRUE(product);
    EXPECT_EQ(product->getNumResults(), 2 + 2 * SHORT_CHAIN);

    expectRows(chain, {{"3"}});
}

TEST_F(PlaceRowInvariantOpsGrowthTest, readsTheSeedOfAHopInFrontOfIt) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'})-->(b) WITH s, b", " WITH s, b WHERE b.age >= s.age", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::GetOutEdges>(*placed._module, "age"), SHORT_CHAIN);

    expectRows(chain, {{"1"}});
}

TEST_F(PlaceRowInvariantOpsGrowthTest, readsTheSeedOfAWalkInFrontOfIt) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'})-[*1..2]->(b) WITH s, b", " WITH s, b WHERE b.age >= s.age", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::ExplorePaths>(*placed._module, "age"), SHORT_CHAIN);

    expectRows(chain, {{"2"}});
}

TEST_F(PlaceRowInvariantOpsGrowthTest, readsTheSeedOfAnOptionalMatchInFrontOfIt) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'}) OPTIONAL MATCH (s)-->(b) WHERE b.age >= s.age", " AND b.age >= s.age", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::OptionalMatch>(*placed._module, "age"), SHORT_CHAIN + 1);
    EXPECT_EQ(countReadsInside<mlir::db::OptionalMatch>(*placed._module, "age"), SHORT_CHAIN + 1);

    expectRows(chain, {{"1"}});
}

TEST_F(PlaceRowInvariantOpsGrowthTest, readsTheInputOfACallInFrontOfIt) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'}) CALL { WITH s MATCH (s)-->(b) WHERE b.age >= s.age", " AND b.age >= s.age", " RETURN b } RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::CallSubquery>(*placed._module, "age"), SHORT_CHAIN + 1);
    EXPECT_EQ(countReadsInside<mlir::db::CallSubquery>(*placed._module, "age"), SHORT_CHAIN + 1);

    expectRows(chain, {{"1"}});
}

// Each sum reads the seed's age, and the comparison the target's, so both the reads and the sums
// are placed in front of the hop
TEST_F(PlaceRowInvariantOpsGrowthTest, computesWhatIsReadOffTheSeedInFrontOfTheHop) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'})-->(b) WITH s, b", " WITH s, b WHERE b.age >= s.age + 1", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::GetOutEdges>(*placed._module, "age"), SHORT_CHAIN);

    const auto isSum = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::AddOp>(op);
    };
    EXPECT_EQ(countBefore<mlir::db::GetOutEdges>(*placed._module, isSum), SHORT_CHAIN);

    expectRows(chain, {{"0"}});
}

// The pattern is walked from the nodes 0, 2 and 7 the unwound values name, so every equality
// compares two columns in front of the hop, and so does each conjunction chaining them, which
// reads the one before it. Nodes 0, 2 and 7 have 5 edges into them.
TEST_F(PlaceRowInvariantOpsGrowthTest, placesAChainOfConjunctionsOfEqualitiesInFrontOfTheHop) {
    const ChainQuery chain {"MATCH (a)-[e]->(b) UNWIND [0, 2, 7] AS y WITH a, b, y", " WITH a, b, y WHERE b = y", " RETURN count(a)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));

    const auto isEquality = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::EqOp>(op);
    };
    const auto isConjunction = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::AndOp>(op);
    };
    EXPECT_EQ(countBefore<mlir::db::GetInEdges>(*placed._module, isEquality), SHORT_CHAIN - 1);
    EXPECT_EQ(countBefore<mlir::db::GetInEdges>(*placed._module, isConjunction), SHORT_CHAIN - 2);

    expectRows(chain, {{"5"}});
}

// Each conjunction reads the one before it, so the pass places one per sweep. Below 1000 clauses a
// sweep costing what the sweeps before it staged stays under the bound, so these chains are twice
// as long as the others. Remy and Adam, the people with an age, have 7 edges out.
TEST_F(PlaceRowInvariantOpsGrowthTest, placesAChainOfConjunctionsOverTheSeedInFrontOfAnOptionalMatch) {
    const ChainQuery chain {"MATCH (s:Person) OPTIONAL MATCH (s)-->(b) WITH s, b", " WITH s, b WHERE s.age > -#", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain, 2 * SHORT_CHAIN, 2 * LONG_CHAIN));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::OptionalMatch>(*placed._module, "age"), SHORT_CHAIN);

    const auto isComparison = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::GtOp>(op);
    };
    const auto isConjunction = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::AndOp>(op);
    };
    EXPECT_EQ(countBefore<mlir::db::OptionalMatch>(*placed._module, isComparison), SHORT_CHAIN);
    EXPECT_EQ(countBefore<mlir::db::OptionalMatch>(*placed._module, isConjunction), SHORT_CHAIN - 1);

    expectRows(chain, {{"7"}});
}

TEST_F(PlaceRowInvariantOpsGrowthTest, placesAChainOfConjunctionsOverTheInputInFrontOfACall) {
    const ChainQuery chain {"MATCH (s:Person) CALL { WITH s MATCH (s)-->(b) RETURN b } WITH s, b", " WITH s, b WHERE s.age > -#", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain, 2 * SHORT_CHAIN, 2 * LONG_CHAIN));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::CallSubquery>(*placed._module, "age"), SHORT_CHAIN);

    const auto isConjunction = [](mlir::Operation* op) {
        return mlir::isa<mlir::db::AndOp>(op);
    };
    EXPECT_EQ(countBefore<mlir::db::CallSubquery>(*placed._module, isConjunction), SHORT_CHAIN - 1);

    expectRows(chain, {{"7"}});
}

// Every projected age is only used after the filter, so each is read for the 3 edges it keeps
TEST_F(PlaceRowInvariantOpsGrowthTest, readsWhatOnlyTheFilteredRowsUseAfterTheFilter) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'})-->(b) WITH b, s.age AS a0", ", s.age AS a#", " WHERE b.name >= 'A' RETURN a0", ", a#"};
    ASSERT_NO_FATAL_FAILURE(expectLinearGrowth(chain));

    PlacedChain placed;
    ASSERT_NO_FATAL_FAILURE(placeChain(chain, placed));
    EXPECT_EQ(countReadsBefore<mlir::db::FilterOp>(*placed._module, "age"), 0u);

    const StringRowSink::Row ages(LONG_CHAIN + 1, "32");
    expectRows(chain, {ages, ages, ages});
}
