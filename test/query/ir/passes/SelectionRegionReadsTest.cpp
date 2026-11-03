#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

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

#include "IRTestOps.h"

using namespace db;
using namespace turing::test;

// A CASE or a coalesce computes its branches and arguments in regions, over the rows it hands
// them. A property is read once per set of rows: a read in the region every row reaches runs
// in front of the op, and one in a region some rows reach takes a column the op carries in.
class SelectionRegionReadsTest : public TuringTest {
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

    void expectNodePropertyReads(std::string_view query, llvm::StringRef property, size_t expected) {
        expectPropertyReads<mlir::db::GetNodeProperties>(query, property, expected);
    }

    void expectEdgePropertyReads(std::string_view query, llvm::StringRef property, size_t expected) {
        expectPropertyReads<mlir::db::GetEdgeProperties>(query, property, expected);
    }

    template <typename ReadOp>
    void expectPropertyReads(std::string_view query, llvm::StringRef property, size_t expected) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;

        size_t count = 0;
        program.walk([&count, property](ReadOp read) {
            if (read.getProperty() == property) {
                count++;
            }
        });

        std::string text;
        print(program, text);
        EXPECT_EQ(count, expected) << query << "\n" << text;
    }

    template <typename ReadOp>
    void expectReads(std::string_view query, size_t expected) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        std::string text;
        print(*module, text);
        EXPECT_EQ(countOps<ReadOp>(*module), expected) << query << "\n" << text;
    }

    void expectNoCarriedConstant(std::string_view query) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = generate(query);
        ASSERT_TRUE(module) << query;

        mlir::ModuleOp program = *module;

        size_t carriedConstants = 0;
        program.walk([&carriedConstants](mlir::Operation* op) {
            if (!mlir::isa<mlir::db::LazyCase, mlir::db::Coalesce>(op)) {
                return;
            }

            for (const mlir::Value carried : op->getOperands()) {
                if (carried.getDefiningOp<mlir::db::ConstantOp>()) {
                    carriedConstants++;
                }
            }
        });

        std::string text;
        print(program, text);
        EXPECT_EQ(carriedConstants, 0u) << query << "\n" << text;
    }

    void print(mlir::ModuleOp module, std::string& text) {
        llvm::raw_string_ostream stream(text);
        module.print(stream);
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(SelectionRegionReadsTest, readsACoalesceArgumentReadBeforeItOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age + 1, coalesce(n.age, 0)", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsACoalesceArgumentReadAfterItOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN coalesce(n.age, 0), n.age + 1", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsAnArgumentLeftFirstByADroppedNullOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN coalesce(null, n.name, n.name + 'x'), n.name", "name", 1);
}

TEST_F(SelectionRegionReadsTest, readsACaseConditionReadAfterItOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN CASE WHEN n.age > 3 THEN 1 END, n.age + 1", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsACaseConditionAndBranchReadBeforeItOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age + 1 AS a, CASE WHEN n.age > 3 THEN n.age END AS b", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsACaseConditionAndItsBranchOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN CASE WHEN n.age > 3 THEN n.age END", "age", 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN CASE WHEN n.age > 3 THEN n.age ELSE n.age + 1 END", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsABranchReadBeforeTheCaseOnce) {
    const std::string_view query = "MATCH (n:Person) RETURN n.age, CASE WHEN n.name = 'Remy' THEN n.age END";
    expectNodePropertyReads(query, "age", 1);
    expectNodePropertyReads(query, "name", 1);
}

TEST_F(SelectionRegionReadsTest, readsALaterCoalesceArgumentReadBeforeItOnce) {
    const std::string_view query = "MATCH (n:Person) RETURN n.age, coalesce(n.name, n.age)";
    expectNodePropertyReads(query, "age", 1);
    expectNodePropertyReads(query, "name", 1);
}

TEST_F(SelectionRegionReadsTest, readsTheSamePropertyInEveryArgumentOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN coalesce(CASE WHEN n.name = 'Remy' THEN n.name END, n.name + '!')",
                            "name",
                            1);
}

TEST_F(SelectionRegionReadsTest, readsTwoPropertiesOfACoalesceOncePerProperty) {
    const std::string_view query = "MATCH (n:Person) RETURN coalesce(n.age, n.name)";
    expectNodePropertyReads(query, "age", 1);
    expectNodePropertyReads(query, "name", 1);
}

TEST_F(SelectionRegionReadsTest, readsACaseInsideACoalesceArgumentOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, coalesce(n.name, CASE WHEN n.age > 3 THEN n.age END)",
                            "age",
                            1);
    expectNodePropertyReads("MATCH (n:Person) RETURN coalesce(CASE WHEN n.age > 3 THEN n.age END, 0)", "age", 1);
}

TEST_F(SelectionRegionReadsTest, readsACoalesceInsideACaseBranchOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, CASE WHEN n.name = 'Remy' THEN coalesce(n.age, 0) END",
                            "age",
                            1);
}

TEST_F(SelectionRegionReadsTest, readsThroughSeveralLevelsOnce) {
    const std::string_view query = "MATCH (n:Person) RETURN n.age, n.dob, "
                                   "CASE WHEN n.name = 'Remy' THEN CASE WHEN n.isFrench THEN coalesce(n.dob, n.age) END END";
    expectNodePropertyReads(query, "age", 1);
    expectNodePropertyReads(query, "dob", 1);
    expectNodePropertyReads(query, "name", 1);
    expectNodePropertyReads(query, "isFrench", 1);
}

TEST_F(SelectionRegionReadsTest, readsThroughDeeplyNestedCasesOnce) {
    constexpr size_t DEPTH = 40;

    std::string query = "MATCH (n:Person) RETURN n.age, ";
    for (size_t level = 0; level < DEPTH; level++) {
        query += "CASE WHEN n.isFrench THEN ";
    }
    query += "n.age";
    for (size_t level = 0; level < DEPTH; level++) {
        query += " END";
    }

    expectNodePropertyReads(query, "age", 1);
    expectNodePropertyReads(query, "isFrench", 1);
}

TEST_F(SelectionRegionReadsTest, readsAnEdgePropertyOnce) {
    expectEdgePropertyReads("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN e.duration + 1, coalesce(e.duration, 0)",
                            "duration",
                            1);
    expectEdgePropertyReads("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN coalesce(e.duration, 0), e.duration + 1",
                            "duration",
                            1);
    expectEdgePropertyReads("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN CASE WHEN e.duration > 15 THEN e.duration END",
                            "duration",
                            1);
    expectEdgePropertyReads("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN e.duration, CASE WHEN e.name = 'x' THEN e.duration END",
                            "duration",
                            1);
}

TEST_F(SelectionRegionReadsTest, readsAPropertyOverTheElementsOfAListOnce) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, [x IN range(1, 3) | x + n.age]", "age", 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, all(x IN range(1, 3) WHERE x < n.age)", "age", 1);
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, reduce(s = 0, x IN range(1, 3) | s + coalesce(n.age, x))",
                            "age",
                            1);
}

// The division stays in front of the op, over every row; the null it adds to is a constant,
// which the op reads from above rather than carrying
TEST_F(SelectionRegionReadsTest, carriesNoConstantOutOfTheFirstRegion) {
    expectNoCarriedConstant("MATCH (n:Person) RETURN coalesce(null + (1 / (size(n.name) - 4)), 2)");
    expectNoCarriedConstant("MATCH (n:Person) RETURN CASE WHEN null + (1 / (size(n.name) - 100)) > 0 THEN 1 END");
}

// A label set or an edge's types read in the region every row reaches shares the read beside
// the op. A read in a later region keeps its own: no chunk kind carries a label set column.
TEST_F(SelectionRegionReadsTest, readsTheLabelSetOfTheFirstConditionOnce) {
    expectReads<mlir::db::GetNodeLabelSet>("MATCH (n) RETURN n:Person, CASE WHEN n:Founder THEN 1 END", 1);
    expectReads<mlir::db::GetNodeLabelSet>("MATCH (n) RETURN CASE WHEN n:Founder THEN 1 END, n:Person", 1);
    expectReads<mlir::db::GetNodeLabelSet>("MATCH (n) RETURN n:Person, coalesce(CASE WHEN n:Founder THEN 1 END, 0)", 1);
}

TEST_F(SelectionRegionReadsTest, readsTheEdgeTypesOfTheFirstConditionOnce) {
    expectReads<mlir::db::GetEdgeTypes>("MATCH ()-[e]->() WITH e RETURN e:KNOWS_WELL, CASE WHEN e:INTERESTED_IN THEN 1 END",
                                        1);
}
