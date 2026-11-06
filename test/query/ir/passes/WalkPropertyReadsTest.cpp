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

// A variable-length walk hands back the node it starts from and the columns it carries,
// row for row with the paths it emits. A property of those rows is read once: a read past
// the walk shares the read made before it.
class WalkPropertyReadsTest : public TuringTest {
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
        llvm::raw_string_ostream stream(text);
        program.print(stream);

        EXPECT_TRUE(mlir::succeeded(mlir::verify(program))) << query << "\n" << text;
        EXPECT_EQ(count, expected) << query << "\n" << text;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalk) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m) WHERE m.age = n.age RETURN m",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m) RETURN n.age, m", "age", 1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkInEveryDirection) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)<-[*1..2]-(m) RETURN n.age, m", "age", 1);
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]-(m) RETURN n.age, m", "age", 1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkFromZeroHops) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*0..2]->(m) RETURN n.age, m", "age", 1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossARepeatedBody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)((a)-[]->(b)-[]->(c)){1,2}(m) RETURN n.age, m",
                            "age",
                            1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkWithAHopPredicate) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)((a)-[e]->(b) WHERE e.duration > 15){1,2}(m) RETURN n.age, m",
                            "age",
                            1);
}

// The walk imports the seed into its hop predicate, an operand standing after the carry set
TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkImportingIt) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "MATCH (n)(()-[e]->() WHERE e.duration < n.age){1,2}(m) RETURN n.age, m",
                            "age",
                            1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkEndingOnANamedNode) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m {name: 'Remy'}) RETURN n.age, m",
                            "age",
                            1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkEndingOnItsSeed) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..3]->(n) RETURN n.age", "age", 1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkEndingOnACarriedNode) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 MATCH (n)-[*1..2]->(m), (n)-->(m) RETURN n.age, m", "age", 1);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkInACountBody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 RETURN COUNT { MATCH (n)-[*1..2]->(m) WHERE m.age = n.age }",
                            "age",
                            2);
}

TEST_F(WalkPropertyReadsTest, readsTheSeedPropertyOnceAcrossAWalkInABodyRunPerRow) {
    expectNodePropertyReads("MATCH (n:Person) "
                            "CALL (n) { MATCH (n)-[*1..2]->(m) WHERE m.age = n.age RETURN m ORDER BY m.name LIMIT 1 } "
                            "RETURN n.age, m",
                            "age",
                            2);
}

TEST_F(WalkPropertyReadsTest, readsACarriedEdgePropertyOnceAcrossAWalk) {
    expectEdgePropertyReads("MATCH (a:Person)-[e]->(b) WHERE e.duration > 15 MATCH (b)-[*1..2]->(c) RETURN e.duration, c",
                            "duration",
                            1);
}
