#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ComputedOnceOverConstantsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void lowerQuery(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& nlModule) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        nlModule = mlir::ModuleOp::create(builder.getUnknownLoc());

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);
    }

    template <typename NLOp>
    void countLoopNesting(std::string_view query, size_t& outsideLoops, size_t& insideLoops) {
        mlir::OwningOpRef<mlir::ModuleOp> nlModule;
        lowerQuery(query, nlModule);

        outsideLoops = 0;
        insideLoops = 0;
        nlModule->walk([&](NLOp op) {
            if (op->template getParentOfType<mlir::nl::For>()) {
                insideLoops++;
            } else {
                outsideLoops++;
            }
        });
    }

    template <typename NLOp>
    void expectComputedOnce(std::string_view query) {
        size_t outsideLoops = 0;
        size_t insideLoops = 0;
        countLoopNesting<NLOp>(query, outsideLoops, insideLoops);

        EXPECT_EQ(outsideLoops, 1u) << "query: " << query;
        EXPECT_EQ(insideLoops, 0u) << "query: " << query;
    }

    template <typename NLOp>
    void expectComputedPerRow(std::string_view query) {
        size_t outsideLoops = 0;
        size_t insideLoops = 0;
        countLoopNesting<NLOp>(query, outsideLoops, insideLoops);

        EXPECT_EQ(outsideLoops, 0u) << "query: " << query;
        EXPECT_EQ(insideLoops, 1u) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(ComputedOnceOverConstantsTest, buildsAConstantRangeOnce) {
    expectComputedOnce<mlir::nl::Range>("UNWIND range(1, 4) AS i RETURN i, size(range(1, 1000))");
}

TEST_F(ComputedOnceOverConstantsTest, slicesAConstantListOnce) {
    expectComputedOnce<mlir::nl::ListSlice>("UNWIND range(1, 4) AS i RETURN i, [1, 2, 3][0..2]");
}

TEST_F(ComputedOnceOverConstantsTest, buildsAListOfConstantsOnce) {
    expectComputedOnce<mlir::nl::MakeList>("UNWIND range(1, 4) AS i RETURN i, [1, toString(2)]");
}

TEST_F(ComputedOnceOverConstantsTest, buildsAMapOfConstantsOnce) {
    expectComputedOnce<mlir::nl::MakeMap>("UNWIND range(1, 4) AS i RETURN i, {a: toString(2)}");
}

TEST_F(ComputedOnceOverConstantsTest, runsStringFunctionsOverConstantsOnce) {
    expectComputedOnce<mlir::nl::Substring>("UNWIND range(1, 4) AS i RETURN i, substring('abcdef', 1, 2)");
    expectComputedOnce<mlir::nl::Split>("UNWIND range(1, 4) AS i RETURN i, split('a,b', ',')");
    expectComputedOnce<mlir::nl::Replace>("UNWIND range(1, 4) AS i RETURN i, replace('abc', 'b', 'x')");
}

TEST_F(ComputedOnceOverConstantsTest, keepsAComprehensionWhoseBodyRaisesOnTheRows) {
    expectComputedPerRow<mlir::nl::ListComprehension>("UNWIND range(1, 4) AS i RETURN i, [x IN [1, 2, 3] | x * 2]");
}

TEST_F(ComputedOnceOverConstantsTest, keepsARangeThatRaisesOnTheRows) {
    expectComputedPerRow<mlir::nl::Range>("UNWIND range(1, 4) AS i RETURN i, range(1, 10, 0)");
}

TEST_F(ComputedOnceOverConstantsTest, keepsACaseWhoseBranchRaisesOnTheRows) {
    expectComputedPerRow<mlir::nl::CaseMerge>("UNWIND range(1, 4) AS i RETURN i, CASE WHEN 1 > 2 THEN 1 / 0 ELSE 0 END");
}

TEST_F(ComputedOnceOverConstantsTest, keepsARangeOverAStoredBoundOnTheRows) {
    expectComputedPerRow<mlir::nl::Range>("MATCH (n:Person) RETURN range(1, n.age)");
}

TEST_F(ComputedOnceOverConstantsTest, projectsAConstantRangeOnEveryRow) {
    expectRows("UNWIND range(1, 3) AS i RETURN i, size(range(1, 1000)), range(1, 3)",
               {{"1", "1000", "[1, 2, 3]"}, {"2", "1000", "[1, 2, 3]"}, {"3", "1000", "[1, 2, 3]"}});
}

TEST_F(ComputedOnceOverConstantsTest, projectsAConstantRangeOnEveryMatchedNode) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n.name, range(1, 2)",
               {{"Adam", "[1, 2]"}, {"Remy", "[1, 2]"}});
}

TEST_F(ComputedOnceOverConstantsTest, testsMembershipInAConstantRange) {
    expectRows("UNWIND range(1, 4) AS i WITH i WHERE i IN range(2, 3) RETURN i", {{"2"}, {"3"}});
}

TEST_F(ComputedOnceOverConstantsTest, unwindsASliceOfAConstantRange) {
    expectRows("UNWIND range(1, 2) AS i UNWIND range(1, 3)[1..] AS j RETURN i, j",
               {{"1", "2"}, {"1", "3"}, {"2", "2"}, {"2", "3"}});
}

TEST_F(ComputedOnceOverConstantsTest, comprehendsAConstantRangeAgainstTheRow) {
    expectRows("UNWIND range(1, 3) AS i RETURN [x IN range(1, 3) | x * i]",
               {{"[1, 2, 3]"}, {"[2, 4, 6]"}, {"[3, 6, 9]"}});
}

TEST_F(ComputedOnceOverConstantsTest, carriesAConstantRangeThroughAFilter) {
    expectRows("UNWIND range(1, 4) AS i WITH i, range(1, 3) AS r WHERE i > 2 RETURN i, r",
               {{"3", "[1, 2, 3]"}, {"4", "[1, 2, 3]"}});
}

TEST_F(ComputedOnceOverConstantsTest, groupsOnAConstantRange) {
    expectRows("UNWIND range(1, 3) AS i RETURN range(1, 2) AS r, count(i)", {{"[1, 2]", "3"}});
}

TEST_F(ComputedOnceOverConstantsTest, collectsAConstantRangeOncePerRow) {
    expectRows("UNWIND range(1, 2) AS i RETURN collect(range(1, 2))", {{"[[1, 2], [1, 2]]"}});
}

TEST_F(ComputedOnceOverConstantsTest, readsAConstantRangeInACaseBranch) {
    expectRows("UNWIND range(1, 3) AS i RETURN i, CASE WHEN i > 1 THEN size(range(1, 2)) ELSE size(range(5, 5)) END",
               {{"1", "1"}, {"2", "2"}, {"3", "2"}});
}

TEST_F(ComputedOnceOverConstantsTest, concatenatesTwoConstantRanges) {
    expectRows("UNWIND range(1, 2) AS i RETURN i, size(range(1, 3) + range(4, 5))", {{"1", "5"}, {"2", "5"}});
}

TEST_F(ComputedOnceOverConstantsTest, projectsStringFunctionsOverConstants) {
    expectRows("UNWIND range(1, 2) AS i RETURN i, substring('abcdef', 1, 2), split('a,b', ','), replace('abc', 'b', 'x')",
               {{"1", "bc", "[a, b]", "axc"}, {"2", "bc", "[a, b]", "axc"}});
}

TEST_F(ComputedOnceOverConstantsTest, projectsContainersOfConstants) {
    expectRows("UNWIND range(1, 2) AS i RETURN i, [1, toString(2)], {a: toString(2)}",
               {{"1", "[1, 2]", "{a: 2}"}, {"2", "[1, 2]", "{a: 2}"}});
}

TEST_F(ComputedOnceOverConstantsTest, projectsAComprehensionOverConstants) {
    expectRows("UNWIND range(1, 2) AS i RETURN i, [x IN [1, 2, 3] WHERE x > 1]", {{"1", "[2, 3]"}, {"2", "[2, 3]"}});
}

TEST_F(ComputedOnceOverConstantsTest, projectsACaseOverConstants) {
    expectRows("UNWIND range(1, 2) AS i RETURN i, CASE WHEN 1 > 0 THEN 'a' ELSE 'b' END", {{"1", "a"}, {"2", "a"}});
}

TEST_F(ComputedOnceOverConstantsTest, raisesNothingForARangeNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN range(1, 10, 0)", {});
}

TEST_F(ComputedOnceOverConstantsTest, raisesOnARangeARowReaches) {
    expectRejected("UNWIND range(1, 2) AS i RETURN i, range(1, 10, 0)", "range() cannot count by a step of 0");
}

TEST_F(ComputedOnceOverConstantsTest, dividesByASafeLiteralOnce) {
    expectComputedOnce<mlir::nl::Div>("UNWIND range(1, 4) AS i RETURN i, 7 / 2");
}

TEST_F(ComputedOnceOverConstantsTest, keepsADivisionByZeroOnTheRows) {
    expectComputedPerRow<mlir::nl::Div>("UNWIND range(1, 4) AS i RETURN i, 1 / 0");
}

TEST_F(ComputedOnceOverConstantsTest, projectsADivisionOfConstantsOnEveryRow) {
    expectRows("UNWIND range(1, 3) AS i RETURN i, 7 / 2", {{"1", "3"}, {"2", "3"}, {"3", "3"}});
}

TEST_F(ComputedOnceOverConstantsTest, raisesNothingForADivisionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN 1 / 0", {});
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN 5 % 0", {});
}

TEST_F(ComputedOnceOverConstantsTest, raisesNothingForADivisionAfterAFilterDropsEveryRow) {
    expectRows("UNWIND range(1, 4) AS i WITH i WHERE i > 10 RETURN i, 1 / 0", {});
}

TEST_F(ComputedOnceOverConstantsTest, countsADivisionNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN count(1 / 0)", {{"0"}});
}

TEST_F(ComputedOnceOverConstantsTest, collectsADivisionOfConstantsOncePerRow) {
    expectRows("UNWIND range(1, 3) AS i RETURN count(*), collect(7 / 2)", {{"3", "[3, 3, 3]"}});
}

TEST_F(ComputedOnceOverConstantsTest, raisesOnADivisionARowReaches) {
    expectRejected("UNWIND range(1, 2) AS i RETURN i, 1 / 0", "Attempted to divide by zero");
}

TEST_F(ComputedOnceOverConstantsTest, raisesOnADivisionOfTheSingleRow) {
    expectRejected("RETURN 1 / 0", "Attempted to divide by zero");
}

TEST_F(ComputedOnceOverConstantsTest, raisesNothingForAMapEntryFunctionNoRowReaches) {
    expectRows("WITH {v: 1} AS m MATCH (n:Person) WHERE n.name = 'Nobody' RETURN properties(m.v)", {});
}
