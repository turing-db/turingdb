#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "PathHopArguments.h"
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

// An op carrying columns into its regions drops the ones no region reads, from its operands
// and from the arguments of every region with a body. A walk's hop imports are such a carry
// set: an import stays while one step reads it, and goes once no step does.
class TrimRegionCarrySetTest : public TuringTest {
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
    mlir::OwningOpRef<mlir::ModuleOp> generate(std::string_view query) {
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
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return owningModule;
    }

    // @param expected holds the hop import count of each walk, in the order the walks stand.
    // Every region with a body takes one argument per import, and some region reads each.
    void expectHopImports(std::string_view query, const std::vector<size_t>& expected) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;
        const std::string text = printed(program);

        std::vector<size_t> importCounts;
        program.walk([&importCounts, &query, &text](mlir::db::ExplorePaths exploration) {
            const mlir::Operation::operand_range imports = exploration.getHopImports();
            importCounts.push_back(imports.size());

            std::vector<bool> read(imports.size(), false);
            for (mlir::Region& hop : exploration.getHops()) {
                if (hop.empty()) {
                    continue;
                }

                mlir::Block& body = hop.front();
                const size_t walkArguments = mlir::hopWalkArgumentCount(hop.getRegionNumber());
                EXPECT_EQ(body.getNumArguments(), walkArguments + imports.size()) << query << "\n" << text;

                for (size_t importIndex = 0; importIndex < imports.size(); importIndex++) {
                    if (!body.getArgument(static_cast<unsigned>(walkArguments + importIndex)).use_empty()) {
                        read[importIndex] = true;
                    }
                }
            }

            for (size_t importIndex = 0; importIndex < imports.size(); importIndex++) {
                EXPECT_TRUE(read[importIndex]) << "hop import " << importIndex << " is unread\n" << query << "\n" << text;
            }
        });

        EXPECT_TRUE(mlir::succeeded(mlir::verify(program))) << query << "\n" << text;
        EXPECT_EQ(importCounts, expected) << query << "\n" << text;
    }

    // @param expected holds the count of columns each @param CarryingOp carries into its
    // regions, in the order the ops stand
    template <typename CarryingOp>
    void expectCarriedColumns(std::string_view query, const std::vector<size_t>& expected) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;
        const std::string text = printed(program);

        std::vector<size_t> carriedCounts;
        program.walk([&carriedCounts](CarryingOp op) {
            carriedCounts.push_back(op.getColumnsToFilter().size());
        });

        EXPECT_TRUE(mlir::succeeded(mlir::verify(program))) << query << "\n" << text;
        EXPECT_EQ(carriedCounts, expected) << query << "\n" << text;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;

    static std::string printed(mlir::ModuleOp program) {
        std::string text;
        llvm::raw_string_ostream stream(text);
        program.print(stream);

        return text;
    }
};

TEST_F(TrimRegionCarrySetTest, dropsTheNodeImportOnceItsPropertyIsCarriedIn) {
    expectHopImports("MATCH (n:Person) WHERE n.age > 0 "
                     "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, dropsTheEdgeImportOnceItsTypeIsCarriedIn) {
    expectHopImports("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
                     "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a, n, m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, keepsAnImportOneStepOfTwoReads) {
    expectHopImports("MATCH (n:Person) WHERE n.age > 0 "
                     "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age AND f.duration > 15){1,2}(m) RETURN m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, dropsAnImportPastAStepWithoutAPredicate) {
    expectHopImports("MATCH (n:Person) WHERE n.age > 0 "
                     "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE f.duration < n.age){1,2}(m) RETURN m",
                     {1});
    expectHopImports("MATCH (n:Person) WHERE n.age > 0 "
                     "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age){1,2}(m) RETURN m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, keepsTheImportsEachStepReads) {
    expectHopImports("MATCH (a)-[r]->(n:Person) WHERE r.duration > 15 AND n.age > 0 "
                     "MATCH (n)((x)-[e]->(y)-[f]->(z) WHERE e.duration < n.age AND f.duration <= r.duration){1}(m) RETURN m",
                     {2});
}

TEST_F(TrimRegionCarrySetTest, keepsEveryImportTheRegionsRead) {
    expectHopImports("MATCH (n:Person) MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN m",
                     {1});
    expectHopImports("MATCH (a)-[r]->(n:Person) "
                     "MATCH (n)(()-[e]->() WHERE e.duration < r.duration AND e.duration < n.age){1,2}(m) RETURN m",
                     {2});
}

TEST_F(TrimRegionCarrySetTest, dropsAnImportAListPredicateFusionLeftUnread) {
    expectHopImports("MATCH (n:Person) WHERE n.age > 0 "
                     "MATCH p = (n)-[*1..2]->(m) WHERE all(x IN relationships(p) WHERE x.duration < n.age) RETURN m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, keepsAnImportAListPredicateFusionAdded) {
    expectHopImports("MATCH (n:Person) "
                     "MATCH p = (n)-[*1..2]->(m) WHERE all(x IN relationships(p) WHERE x.duration < n.age) RETURN m",
                     {1});
}

TEST_F(TrimRegionCarrySetTest, dropsTheColumnsACaseDoesNotRead) {
    expectCarriedColumns<mlir::db::LazyCase>("MATCH (n:Person) RETURN n.name, CASE WHEN n.age = 0 THEN 0 ELSE 10 / n.age END",
                                             {2});
}

TEST_F(TrimRegionCarrySetTest, dropsTheColumnsACoalesceDoesNotRead) {
    expectCarriedColumns<mlir::db::Coalesce>("MATCH (n:Person) RETURN n.name, coalesce(n.age, 10 / n.age)", {1});
}

TEST_F(TrimRegionCarrySetTest, dropsTheColumnsTheListOpsDoNotRead) {
    expectCarriedColumns<mlir::db::ListComprehension>("MATCH (n:Person) RETURN n.name, [x IN [1, 2] | x + n.age]", {1});
    expectCarriedColumns<mlir::db::ListPredicate>("MATCH (n:Person) RETURN n.name, any(x IN [1, 2] WHERE x > n.age)", {1});
    expectCarriedColumns<mlir::db::Reduce>("MATCH (n:Person) RETURN n.name, reduce(s = 0, x IN [1, 2] | s + x + n.age)", {1});
    expectCarriedColumns<mlir::db::AllReduce>("MATCH (n:Person) RETURN n.name, allReduce(s = 0, x IN [1, 2] | s + x, s < n.age)",
                                              {1});
}
