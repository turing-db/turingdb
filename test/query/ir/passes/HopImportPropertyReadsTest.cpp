#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

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

// A hop predicate reading a property of a node or edge the walk imports shares the read of
// that property made before the walk, which comes into every hop region as an import of its
// own. A property nothing reads before the walk is still read in the hop region alone.
class HopImportPropertyReadsTest : public TuringTest {
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

    void expectNodePropertyReads(std::string_view query, llvm::StringRef property, size_t expected, size_t expectedInHops) {
        expectPropertyReads<mlir::db::GetNodeProperties>(query, property, expected, expectedInHops);
    }

    void expectEdgePropertyReads(std::string_view query, llvm::StringRef property, size_t expected, size_t expectedInHops) {
        expectPropertyReads<mlir::db::GetEdgeProperties>(query, property, expected, expectedInHops);
    }

    template <typename ReadOp>
    void expectPropertyReads(std::string_view query, llvm::StringRef property, size_t expected, size_t expectedInHops) {
        const auto readsTheProperty = [property](ReadOp read) {
            return read.getProperty() == property;
        };

        expectReads<ReadOp>(query, readsTheProperty, expected, expectedInHops);
    }

    template <typename ReadOp>
    void expectEveryRead(std::string_view query, size_t expected, size_t expectedInHops) {
        const auto readsAnything = [](ReadOp read) {
            return true;
        };

        expectReads<ReadOp>(query, readsAnything, expected, expectedInHops);
    }

    // @param expectedInHops of the @param expected reads stand in a hop region
    template <typename ReadOp, typename Counted>
    void expectReads(std::string_view query, Counted counted, size_t expected, size_t expectedInHops) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;

        size_t count = 0;
        size_t countInHops = 0;
        program.walk([&count, &countInHops, &counted](ReadOp read) {
            if (!counted(read)) {
                return;
            }

            count++;
            if (read->template getParentOfType<mlir::db::ExplorePaths>()) {
                countInHops++;
            }
        });

        std::string text;
        llvm::raw_string_ostream stream(text);
        program.print(stream);

        EXPECT_TRUE(mlir::succeeded(mlir::verify(program))) << query << "\n" << text;
        EXPECT_EQ(count, expected) << query << "\n" << text;
        EXPECT_EQ(countInHops, expectedInHops) << query << "\n" << text;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(HopImportPropertyReadsTest, readsAnImportedNodePropertyOnce) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN m",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedNodePropertyOnceWhenTheWalkHandsItBack) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN n.age, m",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedEdgePropertyOnce) {
    expectEdgePropertyReads("MATCH (a)-[r]->(n:Person) WHERE r.duration > 15 "
                            "MATCH (n)(()-[e]->() WHERE e.duration < r.duration){1,2}(m) RETURN a, m",
                            "duration",
                            2,
                            1);
    expectEdgePropertyReads("MATCH (a)-[r]->(n:Person) WHERE r.duration > 15 "
                            "MATCH (n)(()-[e]->() WHERE e.duration < r.duration){1,2}(m) RETURN a, r.duration, m",
                            "duration",
                            2,
                            1);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedPropertyOnceBesideAReadOfTheHopNode) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)(()-[e]->(b) WHERE b.age = n.age){1,2}(m) RETURN n.age, m",
                            "age",
                            2,
                            1);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedPropertyOnceInTheLastStepOfABody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE f.duration < n.age){1,2}(m) RETURN m",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedPropertyOnceInTheFirstStepOfABody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age){1,2}(m) RETURN m",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedPropertyOnceInEveryStepOfABody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)((a)-[e]->(b)-[f]->(c) WHERE e.duration < n.age AND f.duration < n.age){1,2}(m) "
                            "RETURN n.age, m",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsAnImportedPropertyOnceInACountBody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "RETURN n.name, COUNT { MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) }",
                            "age",
                            1,
                            0);
}

TEST_F(HopImportPropertyReadsTest, readsTheLabelSetOfAnImportedNodeOnce) {
    expectEveryRead<mlir::db::GetNodeLabelSet>("MATCH (n:Person) WHERE n:Founder OR n.age > 40 WITH n "
                                               "MATCH (n)(()-[e]->() WHERE n:SoftwareEngineering AND e.duration > 15){1,2}(m) RETURN m",
                                               1,
                                               0);
}

TEST_F(HopImportPropertyReadsTest, readsTheTypeOfAnImportedEdgeOnce) {
    expectEveryRead<mlir::db::GetEdgeTypes>("MATCH (a:Person)-[r]->(n) WITH a, r, n WHERE r:KNOWS_WELL OR r.duration > 15 "
                                            "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a, n, m",
                                            1,
                                            0);
}

TEST_F(HopImportPropertyReadsTest, keepsTheReadInTheHopRegionWhenNothingReadsItBefore) {
    expectNodePropertyReads("MATCH (n:Person) MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN m",
                            "age",
                            1,
                            1);
    expectEdgePropertyReads("MATCH (a)-[r]->(n:Person) MATCH (n)(()-[e]->() WHERE e.duration < r.duration){1,2}(m) RETURN a, m",
                            "duration",
                            2,
                            2);
}
