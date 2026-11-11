#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/ADT/StringRef.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBPassPipeline.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "DBRowDependence.h"
#include "ExplainReport.h"
#include "ExplainRequest.h"
#include "HashJoinQueryTest.h"
#include "LocalMemory.h"
#include "NLInterpreter.h"
#include "StringRowSink.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

// An op laid out over the rows another op opened reads a column of them, so the chain from an
// output back to every op shaping its rows is a use-def chain, and an op nothing reads shapes
// no row. MLIR's own dead code elimination then changes no answer, after any pass.
class ExplicitRowDependenceTest : public HashJoinQueryTest {
protected:
    void expectRowDependenceAfterEveryPass(std::string_view query) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> module;
        ASSERT_NO_FATAL_FAILURE(generateUnoptimised(query, view, module));

        std::string diagnostics;
        const mlir::ScopedDiagnosticHandler handler(&_context, [&diagnostics](mlir::Diagnostic& diagnostic) {
            diagnostics += diagnostic.str();
            diagnostics += "\n";
            return mlir::success();
        });

        EXPECT_TRUE(mlir::succeeded(mlir::db::verifyRowDependence(*module))) << query << "\ncodegen: " << diagnostics;

        std::vector<std::string_view> passNames;
        DBPassPipeline::fillPassNames(passNames);

        const mlir::db::DBPassContext passContext = passContextOf(view);
        for (size_t passIndex = 0; passIndex < passNames.size(); passIndex++) {
            ASSERT_NO_FATAL_FAILURE(runPass(passIndex, passContext, *module));

            EXPECT_TRUE(mlir::succeeded(mlir::db::verifyRowDependence(*module)))
                << query << "\nafter " << passNames[passIndex] << ": " << diagnostics;
        }
    }

    // The rows of @param query, through the pipeline or with MLIR's dead code elimination run
    // over the program after codegen and after every pass
    void runQueryEliminatingDeadCode(std::string_view query, bool eliminatesDeadCode, Rows& rows) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> module;
        ASSERT_NO_FATAL_FAILURE(generateUnoptimised(query, view, module));

        const mlir::db::DBPassContext passContext = passContextOf(view);

        for (size_t passIndex = 0; passIndex < countPasses(); passIndex++) {
            if (eliminatesDeadCode) {
                eraseDeadOps(*module);
            }

            ASSERT_NO_FATAL_FAILURE(runPass(passIndex, passContext, *module));
        }

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        lower(view, module.get(), nlModule.get());

        StringRowSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory);
        interpreter.run();

        rows = sink.getRows();
    }

    void expectRowsUnchangedByDeadCodeElimination(std::string_view query) {
        Rows rows;
        Rows eliminatedRows;
        ASSERT_NO_FATAL_FAILURE(runQueryEliminatingDeadCode(query, false, rows));
        ASSERT_NO_FATAL_FAILURE(runQueryEliminatingDeadCode(query, true, eliminatedRows));

        EXPECT_EQ(eliminatedRows, rows) << query;
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    bool verifiesRowDependence(std::string_view ops) {
        std::string program = R"mlir(
!nodes = !db.column<!storage.node_id>
!cells = !db.column<!storage.any>
!counts = !db.column<none>

func.func @main() {
)mlir";

        program += ops;
        program += "\n  return\n}\n";

        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        EXPECT_TRUE(module) << ops;

        return module && mlir::succeeded(mlir::db::verifyRowDependence(*module));
    }

    // MLIR's dead code elimination: every op its generic test calls dead goes, until none is
    static void eraseDeadOps(mlir::ModuleOp module) {
        bool erased = true;
        while (erased) {
            erased = false;
            module->walk<mlir::WalkOrder::PostOrder>([&erased](mlir::Operation* op) {
                if (!mlir::isa<mlir::ModuleOp, mlir::func::FuncOp>(op) && mlir::isOpTriviallyDead(op)) {
                    op->erase();
                    erased = true;
                }
            });
        }
    }

    static size_t countDeadOps(mlir::ModuleOp module) {
        size_t count = 0;
        module->walk([&count](mlir::Operation* op) {
            if (mlir::isOpTriviallyDead(op)) {
                count++;
            }
        });

        return count;
    }

    static size_t countOps(mlir::ModuleOp module, llvm::StringRef name, bool unreadOnly) {
        size_t count = 0;
        module->walk([name, unreadOnly, &count](mlir::Operation* op) {
            const bool counted = op->getName().getStringRef() == name && (!unreadOnly || op->use_empty());
            if (counted) {
                count++;
            }
        });

        return count;
    }

private:
    // Codegen's program for @param query, read back from its dump before any pass ran
    void generateUnoptimised(std::string_view query, const GraphView& view, mlir::OwningOpRef<mlir::ModuleOp>& generated) {
        CypherAST ast(&_procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        ExplainRequest request;
        request.requestStage("codegen");
        ExplainReport report(&request);

        mlir::OwningOpRef<mlir::ModuleOp> optimised = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        mlir::ModuleOp module = optimised.get();

        DBProgramGenerator generator(&module, &report, passContextOf(view));
        generator.generate(&ast);

        ASSERT_EQ(report.getDumps().size(), 1u) << query;

        generated = mlir::parseSourceString<mlir::ModuleOp>(report.getDumps().front(), mlir::ParserConfig(&_context));
        ASSERT_TRUE(generated) << query;
    }

    void runPass(size_t passIndex, const mlir::db::DBPassContext& passContext, mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.enableVerifier(false);
        passManager.addPass(DBPassPipeline::createPass(passIndex, &passContext));

        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
    }

    static size_t countPasses() {
        std::vector<std::string_view> passNames;
        DBPassPipeline::fillPassNames(passNames);

        return passNames.size();
    }

    static mlir::db::DBPassContext passContextOf(const GraphView& view) {
        return mlir::db::DBPassContext {&view, true, true};
    }
};

// Finding 1 of #1451: the unwind of a list a fold made constant carried no column, so the scan
// it runs over had no reader. It carries one now: 2 elements for each of the 8 people.
TEST_F(ExplicitRowDependenceTest, unwindsAFoldedListOverTheScanItReads) {
    const std::string_view query = "MATCH (n:Person) UNWIND {k: [1, 2]}.k AS x RETURN x";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    EXPECT_EQ(countOps(*module, "db.scan_nodes_by_label", false), 1u);
    EXPECT_EQ(countOps(*module, "db.scan_nodes_by_label", true), 0u);
    EXPECT_EQ(countDeadOps(*module), 0u);

    const StringRowSink::Row one {"1"};
    const StringRowSink::Row two {"2"};

    Rows expected(8, one);
    expected.insert(expected.end(), 8, two);

    expectSortedRows(query, expected);
}

// Finding 2 of #1451: the outer unwind_range shapes the rows the inner one multiplies, 8000
// times 1000 of them, and the inner one reads a column of it. The rows are those of the same
// query over smaller ranges.
TEST_F(ExplicitRowDependenceTest, unwindsARangeOverTheRangeBeforeIt) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN 'item ' + toString(j)", module);

    EXPECT_EQ(countOps(*module, "db.unwind_range", false), 2u);
    EXPECT_EQ(countOps(*module, "db.unwind_range", true), 0u);
    EXPECT_EQ(countDeadOps(*module), 0u);

    expectSortedRows("UNWIND range(1, 2) AS i UNWIND range(1, 3) AS j RETURN 'item ' + toString(j)",
                     {{"item 1"}, {"item 1"}, {"item 2"}, {"item 2"}, {"item 3"}, {"item 3"}});
}

// A coalesce whose first argument may raise runs over the rows reaching it, none here, and
// reads a column of the empty unwind
TEST_F(ExplicitRowDependenceTest, coalescesOverTheEmptyUnwindItReads) {
    const std::string_view query = "UNWIND [] AS i RETURN coalesce(100 / 0 + i, 1)";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    EXPECT_EQ(countOps(*module, "db.unwind_const", false), 1u);
    EXPECT_EQ(countOps(*module, "db.unwind_const", true), 0u);
    EXPECT_EQ(countDeadOps(*module), 0u);

    expectSortedRows(query, {});
}

// The hop of the body runs over the one row the count before it leaves, and reads a column of
// it. Remy has 3 interests, Doruk and Martina 1, the others 2.
TEST_F(ExplicitRowDependenceTest, hopsOverTheCountOfACountSubqueryBody) {
    const std::string_view query = "MATCH (p:Person) "
                                   "RETURN p.name, COUNT { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) RETURN i }";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    EXPECT_EQ(countOps(*module, "db.count", false), 1u);
    EXPECT_EQ(countOps(*module, "db.count", true), 0u);
    EXPECT_EQ(countDeadOps(*module), 0u);

    expectSortedRows(query, {{"Adam", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "2"},
                             {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

// Finding 3 of #1451: the count whose key the first map read dropped is erased, so one count runs
TEST_F(ExplicitRowDependenceTest, countsOnceForTheOneKeyRead) {
    const std::string_view query = "MATCH (n:Person) RETURN {x: count(n), x: 1}.x, {x: 1, x: count(n)}.x";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    EXPECT_EQ(countOps(*module, "db.count_scan_rows", false), 1u);
    EXPECT_EQ(countDeadOps(*module), 0u);

    expectSortedRows(query, {{"1", "8"}});
}

TEST_F(ExplicitRowDependenceTest, rejectsAnUnwindReadingNoColumnOfTheScanBeforeIt) {
    EXPECT_FALSE(verifiesRowDependence(R"mlir(
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2]) : !cells
  %element = db.unwind(%xs, {}) : (!cells) -> !cells
  db.output(%element) : !cells
)mlir"));

    EXPECT_TRUE(verifiesRowDependence(R"mlir(
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2]) : !cells
  %element, %carried = db.unwind(%xs, {%n}) : (!cells, !nodes) -> (!cells, !nodes)
  db.output(%element) : !cells
)mlir"));
}

TEST_F(ExplicitRowDependenceTest, rejectsAnOutputOfConstantsAfterAScan) {
    EXPECT_FALSE(verifiesRowDependence(R"mlir(
  %n = db.scan_nodes() : !nodes
  %one = db.constant(1 : i64) : !cells
  db.output(%one) : !cells
)mlir"));

    EXPECT_TRUE(verifiesRowDependence(R"mlir(
  %n = db.scan_nodes() : !nodes
  %one = db.constant(1 : i64) : !cells
  %laidOut = db.broadcast_constant(%one, %n) : (!cells, !nodes) -> !cells
  db.output(%laidOut) : !cells
)mlir"));

    EXPECT_TRUE(verifiesRowDependence(R"mlir(
  %one = db.constant(1 : i64) : !cells
  db.output(%one) : !cells
)mlir"));
}

// A count of a constant after another count reduces the one row the first left
TEST_F(ExplicitRowDependenceTest, rejectsAReductionOfAConstantAfterAnother) {
    EXPECT_FALSE(verifiesRowDependence(R"mlir(
  %one = db.constant(1 : i64) : !cells
  %first = db.count(%one) : (!cells) -> !counts
  %second = db.count(%one) : (!cells) -> !counts
  db.output(%first, %second) : !counts, !counts
)mlir"));

    EXPECT_TRUE(verifiesRowDependence(R"mlir(
  %one = db.constant(1 : i64) : !cells
  %row = db.broadcast_constant(%one) : (!cells) -> !cells
  %first = db.count(%row) : (!cells) -> !counts
  %second = db.count(%row) : (!cells) -> !counts
  db.output(%first, %second) : !counts, !counts
)mlir"));
}

// A source opens rows of its own wherever it stands, reading none of the rows in flight
TEST_F(ExplicitRowDependenceTest, acceptsASourceAfterRows) {
    EXPECT_TRUE(verifiesRowDependence(R"mlir(
  %n = db.scan_nodes() : !nodes
  %first = db.count_scan_rows([["Person"]]) : !counts
  %second = db.count_scan_rows([["Person"]]) : !counts
  db.output(%second) : !counts
)mlir"));
}

TEST_F(ExplicitRowDependenceTest, lowersNoProgramBreakingTheRowDependence) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %xs = db.constant([1, 2]) : !db.column<!storage.any>
  %element = db.unwind(%xs, {}) : (!db.column<!storage.any>) -> !db.column<!storage.any>
  db.output(%element) : !db.column<!storage.any>
  return
}
)mlir",
                                                                                        mlir::ParserConfig(&_context));
    ASSERT_TRUE(module);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
    EXPECT_THROW(lower(view, module.get(), nlModule.get()), TuringException);
}

// The constant of the first branch is laid out over its rows, as the column of the second is
// held: the two outputs declare one type
TEST_F(ExplicitRowDependenceTest, projectsAConstantInOneUnionBranchAndAColumnInTheOther) {
    Rows expected(8, StringRowSink::Row {"x"});
    for (const char* interest : {"Animals", "Bio", "Computers", "Cooking", "Eighties", "Ghosts", "Gym", "JiuJitsu", "Padel", "Travel"}) {
        expected.push_back({interest});
    }

    std::sort(expected.begin(), expected.end());

    expectSortedRows("MATCH (n:Person) RETURN 'x' AS name UNION ALL MATCH (m:Interest) RETURN m.name AS name", expected);
}

class ExplicitRowDependenceSampleTest : public ExplicitRowDependenceTest,
                                        public ::testing::WithParamInterface<std::string_view> {
};

TEST_P(ExplicitRowDependenceSampleTest, holdsAfterCodegenAndEveryPass) {
    expectRowDependenceAfterEveryPass(GetParam());
}

TEST_P(ExplicitRowDependenceSampleTest, answersTheSameWithDeadCodeEliminatedAfterEveryPass) {
    expectRowsUnchangedByDeadCodeElimination(GetParam());
}

TEST_P(ExplicitRowDependenceSampleTest, leavesNoOpNothingReads) {
    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(GetParam(), module);

    EXPECT_EQ(countDeadOps(*module), 0u) << GetParam();
}

INSTANTIATE_TEST_SUITE_P(Queries,
                         ExplicitRowDependenceSampleTest,
                         ::testing::Values("MATCH (n:Person) UNWIND {k: [1, 2]}.k AS x RETURN x",
                                           "UNWIND range(1, 4) AS i UNWIND range(1, 3) AS j RETURN 'item ' + toString(j)",
                                           "UNWIND [] AS i RETURN coalesce(100 / 0 + i, 1)",
                                           "MATCH (p:Person) RETURN p.name, COUNT { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) RETURN i }",
                                           "MATCH (n:Person) RETURN {x: count(n), x: 1}.x, {x: 1, x: count(n)}.x",
                                           "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, 1",
                                           "MATCH (n:Nobody) RETURN 1 / 0",
                                           "MATCH (n:Person)-[:INTERESTED_IN]->(i) RETURN n.name, i.name",
                                           "MATCH (n:Person) RETURN count(*), sum(1), collect(1)",
                                           "RETURN sum(42), count(42)",
                                           "MATCH (n:Person) WITH count(n) AS c MATCH (m:Interest) RETURN c, m.name",
                                           "UNWIND [1, 2, 3] AS x RETURN x, CASE WHEN x > 1 THEN 'big' ELSE 'small' END",
                                           "UNWIND range(1, 4) AS i RETURN i, CASE WHEN 1 > 2 THEN 1 / 0 ELSE 0 END",
                                           "UNWIND [1, 2] AS x UNWIND [3, 4] AS y RETURN x + y",
                                           "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | x * 2]",
                                           "MATCH (n:Person) RETURN n.name, [(n)-[:INTERESTED_IN]->(i) | i.name]",
                                           "MATCH (n:Person) RETURN n.name, size([x IN [0] WHERE x > 0 | 1 / x])",
                                           "MATCH (n:Person) RETURN n.name, reduce(acc = 0, x IN [1, 2, 3] | acc + x)",
                                           "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(k) RETURN n.name, k.name",
                                           "MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:KNOWS_WELL]->() }",
                                           "MATCH (p:Person) CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS c } RETURN p.name, c",
                                           "MATCH (p:Person) CALL (p) { RETURN count(p) AS c, count(DISTINCT p) AS d } RETURN p.name, c, d",
                                           "MATCH (p:Person) CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c "
                                           "UNWIND range(1, c) AS k RETURN size(collect(p)) AS n } RETURN p.name, n",
                                           "MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) "
                                           "CALL (e) { WITH count(*) AS c RETURN e AS k } RETURN b.name, size(k)",
                                           "MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 3",
                                           "MATCH (p:Person) WITH DISTINCT p.age AS age RETURN age",
                                           "MATCH (a:Person), (b:Interest) RETURN count(*)",
                                           "MATCH (n:Person) WITH n, 'age' AS k WHERE n.age = 32 RETURN n.name"));
