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

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The conditions over the rows of one op make one plan, whatever order the query writes them
// in and whether one WHERE or two around a WITH holds them. Each query compiles twice: with the
// graph's counts in the pass context, as the engine runs it, and without them.
class MergeFiltersOverOneOpTest : public TuringTest {
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

    static std::string_view contextName(bool withCounts) {
        return withCounts ? "with counts" : "without counts";
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
    void expectOps(std::string_view query, bool withCounts, size_t expected) {
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

    template <typename... OpTypes>
    void expectOps(std::string_view query, size_t expected) {
        expectOps<OpTypes...>(query, true, expected);
        expectOps<OpTypes...>(query, false, expected);
    }

    // The one division of the program sits in a filter over the rows another filter keeps, so
    // it never sees a row the other removed
    void expectDivisionAfterAFilter(std::string_view query) {
        for (const bool withCounts : {true, false}) {
            mlir::OwningOpRef<mlir::ModuleOp> module;
            generate(query, withCounts, module);

            std::string program;
            render(*module, program);

            llvm::SmallVector<mlir::db::DivOp> divisions;
            module->walk([&divisions](mlir::db::DivOp division) {
                divisions.push_back(division);
            });

            ASSERT_EQ(divisions.size(), 1u) << contextName(withCounts) << "\n" << query << "\n" << program;

            mlir::Operation* reader = divisions.front().getOperation();
            while (reader && !mlir::isa<mlir::db::FilterOp>(reader) && !reader->use_empty()) {
                reader = *reader->getUsers().begin();
            }

            mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(reader);
            ASSERT_TRUE(filter) << contextName(withCounts) << "\n" << query << "\n" << program;

            const mlir::Value filtered = filter.getColumnsToFilter().front();
            EXPECT_TRUE(filtered.getDefiningOp<mlir::db::FilterOp>()) << contextName(withCounts) << "\n" << query << "\n" << program;
        }
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(MergeFiltersOverOneOpTest, scansByPropertyValueBeforeAWalk) {
    const std::vector<std::string_view> queries {
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' AND n.age > 3 RETURN m",
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 AND n.name = 'Remy' RETURN m",
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' WITH n, m WHERE n.age > 3 RETURN m",
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 WITH n, m WHERE n.name = 'Remy' RETURN m",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::ScanNodesByPropertyValue>(queries.front(), 1);
    expectOps<mlir::db::ScanNodesByLabel>(queries.front(), 0);
}

TEST_F(MergeFiltersOverOneOpTest, scansByPropertyValueAlone) {
    const std::vector<std::string_view> queries {
        "MATCH (n:Person) WHERE n.name = 'Remy' AND n.age > 3 RETURN n",
        "MATCH (n:Person) WHERE n.age > 3 AND n.name = 'Remy' RETURN n",
        "MATCH (n:Person) WHERE n.name = 'Remy' WITH n WHERE n.age > 3 RETURN n",
        "MATCH (n:Person) WHERE n.age > 3 WITH n WHERE n.name = 'Remy' RETURN n",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::ScanNodesByPropertyValue>(queries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, scansByAPatternPropertyBesideAWhere) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-[]->(m)-[]->(k) WHERE n.age > 3 RETURN k";

    expectOps<mlir::db::ScanNodesByPropertyValue>(query, 1);
    expectOps<mlir::db::ScanNodesByLabel>(query, 0);

    expectOnePlan({
        "MATCH (n:Person)-[]->(m)-[]->(k) WHERE n.name = 'Remy' AND n.age > 3 RETURN k",
        "MATCH (n:Person)-[]->(m)-[]->(k) WHERE n.age > 3 AND n.name = 'Remy' RETURN k",
    });
}

// The edge type and the label of the interest the hop reaches fuse into the hop whichever
// order they come in, before or after the test of the interest's name
TEST_F(MergeFiltersOverOneOpTest, hopsByTypeAndLabel) {
    expectOps<mlir::db::GetOutEdgesByTypeAndLabel, mlir::db::GetInEdgesByTypeAndLabel>(
        "MATCH (a:Person)-[:INTERESTED_IN]->(e:Interest)-[]->(x) WHERE e.name STARTS WITH 'G' RETURN a, x",
        1);

    const std::vector<std::string_view> queries {
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE r:INTERESTED_IN AND e:Interest AND e.name STARTS WITH 'G' RETURN e",
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE r:INTERESTED_IN AND e.name STARTS WITH 'G' AND e:Interest RETURN e",
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE e:Interest AND r:INTERESTED_IN AND e.name STARTS WITH 'G' RETURN e",
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE e:Interest AND e.name STARTS WITH 'G' AND r:INTERESTED_IN RETURN e",
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE e.name STARTS WITH 'G' AND r:INTERESTED_IN AND e:Interest RETURN e",
        "MATCH (a:Person {name: 'Remy'})-[r]->(e) WHERE e.name STARTS WITH 'G' AND e:Interest AND r:INTERESTED_IN RETURN e",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::GetOutEdgesByTypeAndLabel>(queries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, hopsByType) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e.duration > 15 AND e:KNOWS_WELL RETURN b",
        "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e:KNOWS_WELL AND e.duration > 15 RETURN b",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::GetOutEdgesByType>(queries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, hopsByEndpointLabel) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e.duration > 15 AND b:Interest RETURN b",
        "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE b:Interest AND e.duration > 15 RETURN b",
        "MATCH (a:Person {name: 'Remy'})-[e]->(b) WHERE e.duration > 15 WITH b WHERE b:Interest RETURN b",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::GetOutEdgesByLabel>(queries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, scansEdgesByType) {
    const std::vector<std::string_view> queries {
        "MATCH ()-[e]->() WHERE e.duration > 15 AND e:KNOWS_WELL RETURN e",
        "MATCH ()-[e]->() WHERE e:KNOWS_WELL AND e.duration > 15 RETURN e",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::ScanEdgesByType>(queries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, scansEdgesByEndpointLabels) {
    const std::vector<std::string_view> targetQueries {
        "MATCH (a)-[e]->(b) WHERE e.duration > 15 AND b:Interest RETURN a, b",
        "MATCH (a)-[e]->(b) WHERE b:Interest AND e.duration > 15 RETURN a, b",
    };

    expectOnePlan(targetQueries);
    expectOps<mlir::db::ScanEdges, mlir::db::CheckLabelConstraint>(targetQueries.front(), 0);

    const std::vector<std::string_view> bothEndQueries {
        "MATCH (a:Person)-[e]->(b) WHERE e.duration > 15 AND b:Interest RETURN a, b",
        "MATCH (a:Person)-[e]->(b) WHERE b:Interest AND e.duration > 15 RETURN a, b",
    };

    expectOnePlan(bothEndQueries);
    expectOps<mlir::db::ScanOutEdgesByLabelSrcTgt, mlir::db::ScanInEdgesByLabelSrcTgt>(bothEndQueries.front(), 1);
}

TEST_F(MergeFiltersOverOneOpTest, constrainsTheEndsOfAWalk) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person)-[*1..2]->(b) WHERE b.age > 3 AND b:Founder RETURN a",
        "MATCH (a:Person)-[*1..2]->(b) WHERE b:Founder AND b.age > 3 RETURN a",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::CheckLabelConstraint>(queries.front(), 0);
}

TEST_F(MergeFiltersOverOneOpTest, bindsTheEndOfAWalk) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[*1..2]->(c) WHERE c = a AND c.age >= b.age RETURN a",
        "MATCH (a:Person)-[:KNOWS_WELL]->(b:Person)-[*1..2]->(c) WHERE c.age >= b.age AND c = a RETURN a",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::EqOp>(queries.front(), 0);
}

TEST_F(MergeFiltersOverOneOpTest, scansTheListedNodes) {
    const std::vector<std::string_view> walkQueries {
        "MATCH (n:Person)-[*1..2]->(m) WHERE id(n) = 0 AND n.age > 3 RETURN m",
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 AND id(n) = 0 RETURN m",
        "MATCH (n:Person)-[*1..2]->(m) WHERE n.age > 3 WITH n, m WHERE id(n) = 0 RETURN m",
    };

    expectOnePlan(walkQueries);
    expectOps<mlir::db::ConstScanNodes>(walkQueries.front(), 1);

    const std::vector<std::string_view> scanQueries {
        "MATCH (n) WHERE n.age > 3 AND id(n) IN [0, 1] RETURN n",
        "MATCH (n) WHERE id(n) IN [0, 1] AND n.age > 3 RETURN n",
    };

    expectOnePlan(scanQueries);
    expectOps<mlir::db::ConstScanNodes>(scanQueries.front(), 1);
}

// SimpleGraph is too small for the cost model to take the join, so only the program compiled
// without the graph's counts holds one
TEST_F(MergeFiltersOverOneOpTest, joinsOnTheEquality) {
    const std::vector<std::string_view> queries {
        "MATCH (a:Person), (b:Person) WHERE a.age = b.age AND a.name <> b.name RETURN a, b",
        "MATCH (a:Person), (b:Person) WHERE a.name <> b.name AND a.age = b.age RETURN a, b",
    };

    expectOnePlan(queries);
    expectOps<mlir::db::HashJoin>(queries.front(), false, 1);
}

TEST_F(MergeFiltersOverOneOpTest, keepsADivisionAfterTheFilterItRanAfter) {
    expectDivisionAfterAFilter("MATCH (n:Person) WHERE n.age <> 32 WITH n WHERE 10 / (n.age - 32) > 1 RETURN n");
    expectDivisionAfterAFilter("MATCH (n:Person) WHERE n.age <> 32 AND 10 / (n.age - 32) > 1 RETURN n");

    const std::string_view indexed = "MATCH (n:Person) WHERE n.age <> 32 AND 10 / (n.age - 32) > 1 AND n.name = 'Remy' RETURN n";
    expectDivisionAfterAFilter(indexed);
    expectOps<mlir::db::ScanNodesByPropertyValue>(indexed, 1);
}
