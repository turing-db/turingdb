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

using namespace db;
using namespace turing::test;

// An op with a body hands it some of the rows in flight and gets rows back from it. A
// property is read once per set of rows: a read in the body shares the read of the rows it
// was handed, and a read past the op shares the read of the rows the body handed back.
class SubqueryPropertyReadsTest : public TuringTest {
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

        EXPECT_EQ(count, expected) << query << "\n" << text;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInACountBody) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, COUNT { MATCH (n)-->(m) WHERE m.age = n.age }", "age", 2);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInAnExistsBody) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, EXISTS { MATCH (n)-->(m) WHERE m.age = n.age }", "age", 2);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInACollectBody) {
    const std::string_view query = "MATCH (n:Person) RETURN n.age, "
                                   "COLLECT { MATCH (n)-->(m) WHERE m.age = n.age RETURN m.name }";
    expectNodePropertyReads(query, "age", 2);
    expectNodePropertyReads(query, "name", 1);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInAPatternComprehension) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, [(n)-->(m) WHERE m.age = n.age | m.name]", "age", 2);
}

// A body that dedups, sorts or cuts runs once per row and reads that row as a constant
TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInABodyRunPerRow) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, "
                            "COUNT { MATCH (n)-->(m) WHERE m.age = n.age RETURN DISTINCT m }",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, "
                            "COLLECT { MATCH (n)-->(m) WHERE m.age = n.age RETURN m.name ORDER BY m.name }",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m ORDER BY m.name LIMIT 1 } "
                            "RETURN m",
                            "age",
                            2);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInACallBody) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN m",
                            "age",
                            2);
}

TEST_F(SubqueryPropertyReadsTest, readsAPropertyPastACallOnceWithTheBody) {
    expectNodePropertyReads("MATCH (n:Person) CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN n.age, m",
                            "age",
                            2);
}

// The body runs once per row of n, so each row it yields holds that row's n: the age the body
// read of it comes out beside m
TEST_F(SubqueryPropertyReadsTest, readsAPropertyPastACallRunPerRowOnceWithTheBody) {
    expectNodePropertyReads("MATCH (n:Person) "
                            "CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m ORDER BY m.name LIMIT 1 } "
                            "RETURN n.age, m",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) CALL (n) { WITH n LIMIT 1 RETURN n.age AS a } RETURN n.age, a", "age", 1);
    expectNodePropertyReads("MATCH (n:Person) "
                            "CALL (n) { MATCH (n)-[:INTERESTED_IN]->(i) WITH n, i ORDER BY i.name LIMIT 1 "
                            "MATCH (i)<-[:INTERESTED_IN]-(q:Person) WHERE q.age = n.age RETURN q } "
                            "RETURN n.age, q",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) "
                            "CALL (n) { MATCH (m:Person) WHERE m.age = n.age RETURN m ORDER BY m.name LIMIT 1 } "
                            "RETURN n.age, m",
                            "age",
                            2);
}

TEST_F(SubqueryPropertyReadsTest, readsAReturnedPropertyPastACallOnceWithTheBody) {
    expectNodePropertyReads("MATCH (n:Person) CALL (n) { MATCH (n)-->(m) WHERE m.age > 0 RETURN m } RETURN m.age",
                            "age",
                            1);
    expectNodePropertyReads("MATCH (n:Person) CALL (n) { MATCH (n)-->(m) WHERE m.age > 0 RETURN m ORDER BY m.age LIMIT 1 } "
                            "RETURN m.age",
                            "age",
                            1);
}

TEST_F(SubqueryPropertyReadsTest, readsAPropertyOnceAcrossAnOptionalCall) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "OPTIONAL CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN n.age, m",
                            "age",
                            2);
}

TEST_F(SubqueryPropertyReadsTest, readsAPropertyOnceAcrossAnOptionalMatch) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "OPTIONAL MATCH (n)-->(m) WHERE m.age = n.age RETURN n.age, m",
                            "age",
                            2);
}

// The outer body hands n back as the inner op handed it back, which takes n twice
TEST_F(SubqueryPropertyReadsTest, readsAPropertyOnceAcrossNestedBodies) {
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "CALL (n) { CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN m } RETURN m",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) "
                            "CALL (n) { CALL (n) { MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN m } RETURN n.age, m",
                            "age",
                            2);
    expectNodePropertyReads("MATCH (n:Person) WHERE n.age > 0 "
                            "CALL (n) { OPTIONAL MATCH (n)-->(m) WHERE m.age = n.age RETURN m } RETURN n.age, m",
                            "age",
                            2);
}

TEST_F(SubqueryPropertyReadsTest, readsAPropertyPastACrossProductOnceWithTheFactor) {
    expectNodePropertyReads("MATCH (a:Person), (b:Person) WHERE a.age > 1 RETURN a.age, b", "age", 1);
    expectNodePropertyReads("MATCH (a:Person), (b:Person) WHERE b.age > 1 RETURN a, b.age", "age", 1);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterEdgePropertyOnceInABody) {
    expectEdgePropertyReads("MATCH (a:Person)-[e]->(b) WHERE e.duration > 1 "
                            "RETURN EXISTS { MATCH (b)-->(c) WHERE e.duration > 2 }",
                            "duration",
                            1);
}

TEST_F(SubqueryPropertyReadsTest, readsAnOuterPropertyOnceInANestedBody) {
    expectNodePropertyReads("MATCH (n:Person) RETURN n.age, "
                            "COUNT { MATCH (n)-->(m) WHERE EXISTS { MATCH (m)-->(k) WHERE k.age = n.age } }",
                            "age",
                            2);
}
