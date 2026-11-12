#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBRowSource.h"
#include "StorageDialect.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

// A query of clauses repeated N times: _prefix, N times _clause, then _suffix
struct ChainQuery {
    std::string _prefix;
    std::string _clause;
    std::string _suffix;
};

using PassFactory = std::unique_ptr<mlir::Pass> (*)(const mlir::db::DBPassContext*);

// A pass, the EXPLAIN request for the program it runs on, and the label of that program
struct TimedPass {
    PassFactory _create {nullptr};
    std::string _request;
    std::string _label;
};

const TimedPass rerootAtCheapestNode {mlir::db::createRerootPatternAtCheapestNode,
                                      "before reroot_pattern_at_cheapest_node",
                                      "before reroot_pattern_at_cheapest_node"};

const TimedPass foldConstantMapKeys {mlir::db::createFoldConstantMapKeys,
                                     "before fold_constant_map_keys",
                                     "before fold_constant_map_keys 1"};

const TimedPass pushDownFilters {[](const mlir::db::DBPassContext*) { return mlir::db::createPushDownFilters(); },
                                 "before push_down_filters",
                                 "before push_down_filters 1"};

const PassFactory removeRedundantLabelChecks = [](const mlir::db::DBPassContext*) { return mlir::db::createRemoveRedundantLabelChecks(); };

}

// Each query repeats a filter over columns a pass climbs to where their rows come from:
// reroot_pattern_at_cheapest_node to the nodes and edges a pattern bound, fold_constant_map_keys
// to the constant a key is, push_down_filters to where a variable was bound and
// remove_redundant_label_checks to the labels a node is known to carry. Four times the filters
// must take the pass about four times as long, not the sixteen it took when each filter climbed
// the chain anew.
class RowSourceClimbGrowthTest : public CallV3Test {
protected:
    static constexpr size_t SHORT_CHAIN = 1000;
    static constexpr size_t LONG_CHAIN = 4000;
    static constexpr size_t SHORT_FILTER_STACK = 10000;
    static constexpr size_t LONG_FILTER_STACK = 40000;
    static constexpr size_t TIMED_RUNS = 5;
    static constexpr double LINEAR_GROWTH_BOUND = 8.0;

    struct ParsedProgram {
        mlir::MLIRContext _context;
        mlir::OwningOpRef<mlir::ModuleOp> _module;
    };

    void initialize() override {
        CallV3Test::initialize();

        _graph = db::Graph::create();
        db::SimpleGraph::createSimpleGraph(_graph.get());
    }

    static void buildQuery(const ChainQuery& chain, size_t length, std::string& query) {
        query = chain._prefix;
        for (size_t index = 0; index < length; index++) {
            query += chain._clause;
        }

        query += chain._suffix;
    }

    void programBeforeThePass(const ChainQuery& chain, size_t length, const TimedPass& pass, std::string& program) {
        std::string query;
        buildQuery(chain, length, query);

        StringRowSink sink;
        runQuery("EXPLAIN (" + pass._request + ") " + query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == pass._label) {
                program = row.back();
            }
        }
    }

    static void parseProgram(const std::string& program, ParsedProgram& parsed) {
        parsed._context.getOrLoadDialect<mlir::func::FuncDialect>();
        parsed._context.getOrLoadDialect<mlir::storage::Storage>();
        parsed._context.getOrLoadDialect<mlir::db::DB>();

        parsed._module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&parsed._context));
        ASSERT_TRUE(parsed._module) << program;
    }

    void runPass(const std::string& program, PassFactory createPass, double& seconds) {
        ParsedProgram parsed;
        ASSERT_NO_FATAL_FAILURE(parseProgram(program, parsed));

        const db::FrozenCommitTx transaction = _graph->openTransaction();
        const db::GraphReader reader = transaction.readGraph();
        const db::GraphView view = reader.getView();
        const mlir::db::DBPassContext passContext {._view = &view};

        mlir::PassManager passManager(&parsed._context);
        passManager.enableVerifier(false);
        passManager.addPass(createPass(&passContext));

        const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        ASSERT_TRUE(mlir::succeeded(passManager.run(*parsed._module)));
        const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        seconds = std::chrono::duration<double>(end - start).count();
    }

    void fastestPassRun(const std::string& program, PassFactory createPass, double& seconds) {
        seconds = 0;
        for (size_t run = 0; run < TIMED_RUNS; run++) {
            double runSeconds = 0;
            ASSERT_NO_FATAL_FAILURE(runPass(program, createPass, runSeconds));

            seconds = run == 0 ? runSeconds : std::min(seconds, runSeconds);
        }
    }

    void expectLinearPass(const ChainQuery& chain, const TimedPass& pass) {
        std::string shortProgram;
        std::string longProgram;
        ASSERT_NO_FATAL_FAILURE(programBeforeThePass(chain, SHORT_CHAIN, pass, shortProgram));
        ASSERT_NO_FATAL_FAILURE(programBeforeThePass(chain, LONG_CHAIN, pass, longProgram));

        double shortSeconds = 0;
        double longSeconds = 0;
        ASSERT_NO_FATAL_FAILURE(fastestPassRun(shortProgram, pass._create, shortSeconds));
        ASSERT_NO_FATAL_FAILURE(fastestPassRun(longProgram, pass._create, longSeconds));

        EXPECT_LT(longSeconds, LINEAR_GROWTH_BOUND * shortSeconds)
            << SHORT_CHAIN << " filters: " << shortSeconds << " s, " << LONG_CHAIN << " filters: " << longSeconds << " s";
    }

    // A scan under @param height filters, each keeping the rows of the one below it
    static void buildFilterStack(size_t height, std::string& program) {
        program = "func.func @main() {\n  %n0 = db.scan_nodes() : !db.column<!storage.node_id>\n";
        for (size_t level = 1; level <= height; level++) {
            const std::string index = std::to_string(level);
            const std::string below = "%n" + std::to_string(level - 1);

            program += "  %m" + index + " = db.eq " + below + ", " + below
                     + " : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>\n";
            program += "  %n" + index + " = db.filter(%m" + index + ", {" + below
                     + "}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n";
        }

        program += "  db.output(%n" + std::to_string(height) + ") : !db.column<!storage.node_id>\n  return\n}\n";
    }

    // A label scan under @param height filters, each keeping the rows of the one below it that
    // carry the label the scan reads and equal themselves
    static void buildLabelCheckStack(size_t height, std::string& program) {
        const std::string nodes = "!db.column<!storage.node_id>";
        const std::string mask = "!db.column<!storage.bool>";

        program = "func.func @main() {\n  %n0 = db.scan_nodes_by_label([\"Person\"]) : " + nodes + "\n";
        for (size_t level = 1; level <= height; level++) {
            const std::string index = std::to_string(level);
            const std::string below = "%n" + std::to_string(level - 1);

            program += "  %l" + index + " = db.get_node_label_set(" + below + ") : (" + nodes + ") -> !db.column<!storage.labelset_id>\n";
            program += "  %c" + index + " = db.check_label_constraint(%l" + index + ", [\"Person\"]) : (!db.column<!storage.labelset_id>) -> " + mask + "\n";
            program += "  %e" + index + " = db.eq " + below + ", " + below + " : (" + nodes + ", " + nodes + ") -> " + mask + "\n";
            program += "  %m" + index + " = db.and %c" + index + ", %e" + index + " : (" + mask + ", " + mask + ") -> " + mask + "\n";
            program += "  %n" + index + " = db.filter(%m" + index + ", {" + below + "}) : (" + mask + ", " + nodes + ") -> " + nodes + "\n";
        }

        program += "  db.output(%n" + std::to_string(height) + ") : " + nodes + "\n  return\n}\n";
    }

    static void countChecksLeft(const std::string& program, PassFactory createPass, size_t& checks) {
        ParsedProgram parsed;
        ASSERT_NO_FATAL_FAILURE(parseProgram(program, parsed));

        mlir::PassManager passManager(&parsed._context);
        passManager.addPass(createPass(nullptr));
        ASSERT_TRUE(mlir::succeeded(passManager.run(*parsed._module)));

        checks = 0;
        parsed._module->walk([&checks](mlir::db::CheckLabelConstraint check) {
            checks++;
        });
    }

    static bool isFilterResult(mlir::Value column) {
        return mlir::isa_and_present<mlir::db::FilterOp>(column.getDefiningOp());
    }

    // Climbs every filter of the stack to the scan under it, bottom first
    static void climbFilterStack(mlir::ModuleOp module, double& seconds) {
        mlir::db::RowSourceClimbs climbs(isFilterResult);
        mlir::Value scanned;
        size_t climbedToTheScan = 0;
        size_t filterCount = 0;

        const std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        module->walk([&climbs, &scanned, &climbedToTheScan, &filterCount](mlir::Operation* op) {
            if (mlir::db::ScanNodes scan = mlir::dyn_cast<mlir::db::ScanNodes>(op)) {
                scanned = scan.getResult();
            } else if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(op)) {
                filterCount++;
                if (climbs.climb(filter->getResult(0)) == scanned) {
                    climbedToTheScan++;
                }
            }
        });
        const std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        EXPECT_EQ(climbedToTheScan, filterCount);
        seconds = std::chrono::duration<double>(end - start).count();
    }

    static void fastestFilterStackClimb(size_t height, double& seconds) {
        std::string program;
        buildFilterStack(height, program);

        ParsedProgram parsed;
        ASSERT_NO_FATAL_FAILURE(parseProgram(program, parsed));

        seconds = 0;
        for (size_t run = 0; run < TIMED_RUNS; run++) {
            double runSeconds = 0;
            climbFilterStack(*parsed._module, runSeconds);

            seconds = run == 0 ? runSeconds : std::min(seconds, runSeconds);
        }
    }

    void expectRows(const ChainQuery& chain, const std::string& count) {
        std::string query;
        buildQuery(chain, LONG_CHAIN, query);

        StringRowSink sink;
        runQuery(query, sink);

        const std::vector<StringRowSink::Row> expected {{count}};
        EXPECT_EQ(sink.getRows(), expected);
    }

    std::unique_ptr<db::Graph> _graph;
};

TEST_F(RowSourceClimbGrowthTest, rerootsAtASeedEquatedAlongALongChain) {
    const ChainQuery chain {"MATCH (s:Founder) WITH s MATCH (n)-->(m) WITH s, n, m", " WITH s, n, m WHERE n = s", " RETURN count(m)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "7");
}

TEST_F(RowSourceClimbGrowthTest, equatesTheHopEndsAlongALongChain) {
    const ChainQuery chain {"MATCH (a)-->(b) WITH a, b", " WITH a, b WHERE b = a", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "0");
}

TEST_F(RowSourceClimbGrowthTest, boundsAnExplorationEndAlongALongChain) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'}) MATCH (a)-[*1..2]->(b) WITH s, a, b", " WITH s, a, b WHERE b = s", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "3");
}

TEST_F(RowSourceClimbGrowthTest, pushesALongChainOfFiltersOnTheHopStart) {
    const ChainQuery chain {"MATCH (a)-->(b) WITH a, b", " WITH a, b WHERE a.name <> 'Remy'", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "14");
}

// A filter that may raise runs only once every filter written ahead of it holds, so each one
// waits for the one before it
TEST_F(RowSourceClimbGrowthTest, appliesALongChainOfRaisingFiltersInOrder) {
    const ChainQuery chain {"MATCH (a)-->(b) WITH a, b", " WITH a, b WHERE 100 / a.age >= 0", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "7");
}

// Remy and Adam are the only people with an age, both 32, so no one an edge reaches is older
TEST_F(RowSourceClimbGrowthTest, joinsEachSideOfAJoinChain) {
    const ChainQuery chain {"MATCH (s {name: 'Adam'}) MATCH (a)-->(b) WITH s, a, b", " WITH s, a, b WHERE b.age > s.age", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, rerootAtCheapestNode));

    expectRows(chain, "0");
}

TEST_F(RowSourceClimbGrowthTest, foldsTheKeyOfAMapAlongALongChain) {
    const ChainQuery chain {"WITH {a: 1} AS m, 'a' AS k", " WITH m, k WHERE m[k] = 1", " RETURN count(*)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, foldConstantMapKeys));

    expectRows(chain, "1");
}

// Each filter's column climbs one step and takes the answer of the filter below it
TEST_F(RowSourceClimbGrowthTest, climbsEachFilterOfATallStackOnce) {
    double shortSeconds = 0;
    double longSeconds = 0;
    ASSERT_NO_FATAL_FAILURE(fastestFilterStackClimb(SHORT_FILTER_STACK, shortSeconds));
    ASSERT_NO_FATAL_FAILURE(fastestFilterStackClimb(LONG_FILTER_STACK, longSeconds));

    EXPECT_LT(longSeconds, LINEAR_GROWTH_BOUND * shortSeconds)
        << SHORT_FILTER_STACK << " filters: " << shortSeconds << " s, " << LONG_FILTER_STACK << " filters: " << longSeconds << " s";
}

// No filter moves: each equates the hop's two ends, which no one op makes. Every climb crosses
// the same equality of the hop's anchors at each filter above it and keeps it once.
TEST_F(RowSourceClimbGrowthTest, keepsOneEqualityAlongALongChainOfHopEnds) {
    const ChainQuery chain {"MATCH (a)-->(b) WITH a, b", " WITH a, b WHERE b = a", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, pushDownFilters));

    expectRows(chain, "0");
}

// Each filter moves right after the scan, and the climbs of the filters below it still hold
TEST_F(RowSourceClimbGrowthTest, pushesALongChainOfFiltersOntoTheScan) {
    const ChainQuery chain {"MATCH (a)-->(b) WITH a, b", " WITH a, b WHERE a.name <> 'Remy'", " RETURN count(b)"};
    ASSERT_NO_FATAL_FAILURE(expectLinearPass(chain, pushDownFilters));

    expectRows(chain, "14");
}

// Each check climbs to the scan through every filter above it, the checks of which are gone
TEST_F(RowSourceClimbGrowthTest, removesTheLabelChecksOfATallStack) {
    std::string shortProgram;
    std::string longProgram;
    buildLabelCheckStack(SHORT_CHAIN, shortProgram);
    buildLabelCheckStack(LONG_CHAIN, longProgram);

    double shortSeconds = 0;
    double longSeconds = 0;
    ASSERT_NO_FATAL_FAILURE(fastestPassRun(shortProgram, removeRedundantLabelChecks, shortSeconds));
    ASSERT_NO_FATAL_FAILURE(fastestPassRun(longProgram, removeRedundantLabelChecks, longSeconds));

    EXPECT_LT(longSeconds, LINEAR_GROWTH_BOUND * shortSeconds)
        << SHORT_CHAIN << " filters: " << shortSeconds << " s, " << LONG_CHAIN << " filters: " << longSeconds << " s";

    size_t checks = 0;
    ASSERT_NO_FATAL_FAILURE(countChecksLeft(longProgram, removeRedundantLabelChecks, checks));
    EXPECT_EQ(checks, 0u);
}
