#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestOps.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

bool testsTheRows(mlir::Operation* op) {
    return mlir::isa<mlir::db::FilterOp,
                     mlir::db::GetNodeLabelSet,
                     mlir::db::CheckLabelConstraint,
                     mlir::db::Labels,
                     mlir::db::GetEdgeTypes,
                     mlir::db::CheckEdgeTypeConstraint,
                     mlir::db::EdgeType>(op);
}

}

// A WHERE testing the labels of a node, the type of an edge or a function of a property
// filters the rows a walk or a hop starts from before the walk, as a WHERE comparing a
// property does. A WHERE that may raise, or reads what the walk binds, stays below it. Each
// query compiles twice: with the graph's counts in the pass context, as the engine runs it,
// and without them.
class PushDownLabelAndTypeTestsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

protected:
    using Plan = std::vector<std::string>;

    void generate(std::string_view query, bool withCounts, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        mlir::ModuleOp moduleOp = module.get();

        const mlir::db::DBPassContext passContext {._view = withCounts ? &view : nullptr};
        DBProgramGenerator generator(&moduleOp, nullptr, passContext);
        generator.generate(&ast);
    }

    static void render(mlir::ModuleOp module, std::string& program) {
        program.clear();
        llvm::raw_string_ostream stream(program);

        mlir::OpPrintingFlags flags;
        flags.assumeVerified();
        module.print(stream, flags);
    }

    static std::string_view contextName(bool withCounts) {
        return withCounts ? "with counts" : "without counts";
    }

    // Each op of the program as its name and attributes, sorted, so two programs that differ
    // only in the order of their conjuncts read the same
    static void describePlan(mlir::ModuleOp module, Plan& plan) {
        module.walk([&plan](mlir::Operation* op) {
            if (mlir::isa<mlir::ModuleOp, mlir::func::FuncOp>(op)) {
                return;
            }

            std::string entry;
            llvm::raw_string_ostream stream(entry);
            stream << op->getName().getStringRef() << " " << op->getAttrDictionary();
            plan.push_back(entry);
        });

        std::sort(plan.begin(), plan.end());
    }

    // No filter and no label or type test stands after the one @param Walk of the query. A
    // test in the walk's hop region is the hop predicate's own.
    template <typename Walk>
    void expectTestedBeforeTheWalk(std::string_view query) {
        expectTheWalk<Walk>(query, false);
    }

    // As expectTestedBeforeTheWalk, and the walk reads its seeds off a filter
    template <typename Walk>
    void expectSeedsFilteredBeforeTheWalk(std::string_view query) {
        expectTheWalk<Walk>(query, true);
    }

    template <typename Walk>
    void expectTheWalk(std::string_view query, bool seedsFiltered) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> module;
            generate(query, withCounts, module);

            mlir::ModuleOp program = *module;

            std::string text;
            render(program, text);

            const std::string_view context = contextName(withCounts);
            ASSERT_TRUE(mlir::succeeded(mlir::verify(program))) << context << "\n" << query << "\n" << text;

            llvm::SmallVector<Walk> walks = collect<Walk>(program);
            ASSERT_EQ(walks.size(), 1u) << context << "\n" << query << "\n" << text;

            mlir::Operation* const walk = walks.front().getOperation();
            mlir::Block* const walkBlock = walk->getBlock();

            size_t testsAfter = 0;
            program.walk([walk, walkBlock, &testsAfter](mlir::Operation* op) {
                if (!testsTheRows(op)) {
                    return;
                }

                mlir::Operation* const ancestor = walkBlock->findAncestorOpInBlock(*op);
                if (ancestor && walk->isBeforeInBlock(ancestor)) {
                    testsAfter++;
                }
            });

            EXPECT_EQ(testsAfter, 0u) << context << "\n" << query << "\n" << text;

            if (seedsFiltered) {
                const mlir::Value seeds = walk->getOperand(0);
                EXPECT_TRUE(seeds.getDefiningOp<mlir::db::FilterOp>()) << context << "\n" << query << "\n" << text;
            }
        }
    }

    template <typename Walk>
    void expectFilterBelowTheWalk(std::string_view query) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> module;
            generate(query, withCounts, module);

            mlir::ModuleOp program = *module;

            std::string text;
            render(program, text);

            const std::string_view context = contextName(withCounts);
            ASSERT_TRUE(mlir::succeeded(mlir::verify(program))) << context << "\n" << query << "\n" << text;

            llvm::SmallVector<Walk> walks = collect<Walk>(program);
            ASSERT_EQ(walks.size(), 1u) << context << "\n" << query << "\n" << text;

            mlir::Operation* const walk = walks.front().getOperation();
            mlir::Block* const walkBlock = walk->getBlock();

            size_t filtersBelow = 0;
            program.walk([walk, walkBlock, &filtersBelow](mlir::db::FilterOp filter) {
                mlir::Operation* const ancestor = walkBlock->findAncestorOpInBlock(*filter);
                if (ancestor && walk->isBeforeInBlock(ancestor)) {
                    filtersBelow++;
                }
            });

            EXPECT_EQ(filtersBelow, 1u) << context << "\n" << query << "\n" << text;
        }
    }

    void expectOnePlan(const std::vector<std::string_view>& queries) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> first;
            generate(queries.front(), withCounts, first);

            Plan firstPlan;
            describePlan(*first, firstPlan);

            std::string firstProgram;
            render(*first, firstProgram);

            for (const std::string_view query : queries) {
                mlir::OwningOpRef<mlir::ModuleOp> module;
                generate(query, withCounts, module);

                Plan plan;
                describePlan(*module, plan);

                std::string program;
                render(*module, program);

                EXPECT_EQ(plan, firstPlan) << contextName(withCounts) << "\n"
                                           << query << "\n" << program
                                           << "\nagainst " << queries.front() << "\n" << firstProgram;
            }
        }
    }

    template <typename... OpTypes>
    void expectOps(std::string_view query, size_t expected) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> module;
            generate(query, withCounts, module);

            size_t count = 0;
            module->walk([&count](mlir::Operation* op) {
                if (mlir::isa<OpTypes...>(op)) {
                    count++;
                }
            });

            std::string program;
            render(*module, program);

            EXPECT_EQ(count, expected) << contextName(withCounts) << "\n" << query << "\n" << program;
        }
    }

    void expectLabelSetReads(std::string_view query, size_t expected, size_t expectedInHops) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> module;
            generate(query, withCounts, module);

            size_t count = 0;
            size_t countInHops = 0;
            module->walk([&count, &countInHops](mlir::db::GetNodeLabelSet read) {
                count++;
                if (read->getParentOfType<mlir::db::ExplorePaths>()) {
                    countInHops++;
                }
            });

            std::string program;
            render(*module, program);

            EXPECT_EQ(count, expected) << contextName(withCounts) << "\n" << query << "\n" << program;
            EXPECT_EQ(countInHops, expectedInHops) << contextName(withCounts) << "\n" << query << "\n" << program;
        }
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(PushDownLabelAndTypeTestsTest, scansTheLabelledNodesBeforeAWalk) {
    for (const std::string_view query : {"MATCH (n:Person) WHERE n:Founder MATCH (n)-[*1..2]->(m) RETURN m",
                                         "MATCH (n:Person)-[*1..2]->(m) WHERE n:Founder RETURN m"}) {
        expectTestedBeforeTheWalk<mlir::db::ExplorePaths>(query);
        expectOps<mlir::db::ScanNodesByLabel>(query, 1);
        expectOps<mlir::db::CheckLabelConstraint>(query, 0);
    }
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnALabelTestInAnOrBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE n:Founder OR n.age > 40 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE n.age > 40 OR n:Founder "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnALabelTestInAnAndBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE n:Founder AND n.age > 3 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE n.age > 3 AND n:Founder "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE (n:Founder AND n.age > 3) OR n.name = 'Luc' "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnANegatedLabelTestBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE NOT n:Founder "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnTheirLabelsBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE 'Founder' IN labels(n) OR n.age > 40 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnAFunctionOfAPropertyBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE toLower(n.name) = 'remy' "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE size(n.name) > 4 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE toInteger(n.age) > 3 OR n:Founder "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, keepsATestThatMayRaiseBelowTheWalk) {
    expectFilterBelowTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) WHERE n:Founder OR 10 / n.age > 0 "
                                                     "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnALabelTestBeforeAHop) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdges>("MATCH (n:Person) WHERE n:Founder OR n.age > 40 "
                                                            "MATCH (n)-[]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdges>("MATCH (n:Person) WHERE n:Founder AND n.age > 3 "
                                                            "MATCH (n)-[]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdges>("MATCH (n:Person) WHERE NOT n:Founder "
                                                            "MATCH (n)-[]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnALabelTestBeforeAHopByType) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdgesByType>("MATCH (n:Person) WHERE n:Founder OR n.age > 40 "
                                                                  "MATCH (n)-[:INTERESTED_IN]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, hopsByTypeBeforeAWalk) {
    for (const std::string_view query : {"MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL MATCH (n)-[*1..2]->(m) RETURN m",
                                         "MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL "
                                         "MATCH (n)(()-[e]->() WHERE e.duration > 15){1}(m) RETURN a, n, m"}) {
        expectTestedBeforeTheWalk<mlir::db::ExplorePaths>(query);
        expectOps<mlir::db::GetOutEdgesByType>(query, 1);
        expectOps<mlir::db::CheckEdgeTypeConstraint>(query, 0);
    }
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnATypeTestInAnOrBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
                                                             "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) "
                                                             "RETURN a, n, m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnATypeTestOrALabelTestOfTheEdgeEndBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR n:Founder "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnANegatedTypeTestBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) WHERE NOT r:KNOWS_WELL "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnTheirTypeBeforeAWalk) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) WHERE type(r) = 'KNOWS_WELL' OR r.duration > 15 "
                                                             "MATCH (n)-[*1..2]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, filtersTheSeedsOnATypeTestBeforeAHop) {
    expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdges>("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
                                                            "MATCH (n)-[]->(m) RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, keepsALabelTestOfTheWalkEndBelowTheWalk) {
    expectFilterBelowTheWalk<mlir::db::ExplorePaths>("MATCH (n:Person) MATCH (n)-[*1..2]->(m) "
                                                     "WHERE m:Founder OR n.age > 40 RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, keepsATypeTestBesideTheWalkEndBelowTheWalk) {
    expectFilterBelowTheWalk<mlir::db::ExplorePaths>("MATCH (a:Person)-[r]->(n) MATCH (n)-[*1..2]->(m) "
                                                     "WHERE r:KNOWS_WELL OR m:Founder RETURN m");
}

TEST_F(PushDownLabelAndTypeTestsTest, testsEachHopOnTheLabelSetReadBeforeTheWalk) {
    expectLabelSetReads("MATCH (n:Person) WHERE n:Founder OR n.age > 40 "
                        "MATCH (n)(()-[e]->() WHERE n:SoftwareEngineering AND e.duration > 15){1,2}(m) RETURN m",
                        1,
                        0);
}

TEST_F(PushDownLabelAndTypeTestsTest, hopsByTypeAndLabelInEveryOrder) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE r:INTERESTED_IN AND e:Interest AND e.name STARTS WITH 'G' RETURN a, x",
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE r:INTERESTED_IN AND e.name STARTS WITH 'G' AND e:Interest RETURN a, x",
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e:Interest AND r:INTERESTED_IN AND e.name STARTS WITH 'G' RETURN a, x",
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e:Interest AND e.name STARTS WITH 'G' AND r:INTERESTED_IN RETURN a, x",
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e.name STARTS WITH 'G' AND r:INTERESTED_IN AND e:Interest RETURN a, x",
        "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e.name STARTS WITH 'G' AND e:Interest AND r:INTERESTED_IN RETURN a, x",
    };

    expectOnePlan(queries);

    for (const std::string_view query : queries) {
        expectSeedsFilteredBeforeTheWalk<mlir::db::GetOutEdges>(query);
        expectOps<mlir::db::CheckEdgeTypeConstraint, mlir::db::CheckLabelConstraint>(query, 0);
    }
}

TEST_F(PushDownLabelAndTypeTestsTest, scansByPropertyValueAndLabelBeforeAWalk) {
    for (const std::string_view query : {"MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' AND n:Founder RETURN m",
                                         "MATCH (n:Person)-[*1..2]->(m) WHERE n:Founder AND n.name = 'Remy' RETURN m"}) {
        expectTestedBeforeTheWalk<mlir::db::ExplorePaths>(query);
        expectOps<mlir::db::ScanNodesByPropertyValue>(query, 1);
        expectOps<mlir::db::CheckLabelConstraint>(query, 0);
    }
}

// A label test of the node an edge starts from folds into the labels the edge scan reads it by,
// as it does when nothing moves the test before the hop
TEST_F(PushDownLabelAndTypeTestsTest, scansTheEdgesOfTheLabelledNodes) {
    for (const std::string_view query : {"MATCH (n:Person)-->(m:Interest) WHERE n:Founder RETURN n.name, m.name",
                                         "MATCH (n)-->(m:Interest) WHERE n:Bioinformatics RETURN n.name, m.name",
                                         "MATCH (x)-->(y:Person)-->(x:Person) RETURN x.name, y.name"}) {
        expectOps<mlir::db::ScanOutEdgesByLabelSrcTgt, mlir::db::ScanInEdgesByLabelSrcTgt>(query, 1);
        expectOps<mlir::db::CheckLabelConstraint>(query, 0);
    }
}
