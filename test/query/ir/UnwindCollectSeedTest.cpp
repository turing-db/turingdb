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
#include "DBDialectInterpreter.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

template <typename OpType>
size_t countOps(mlir::ModuleOp module) {
    size_t count = 0;
    module.walk([&count](OpType) {
        count++;
    });

    return count;
}

}

// A MATCH that walks out of a variable an UNWIND bound expands the rows the unwind emitted
// rather than the graph's: the elements open the traversal, the way a CALL's yielded nodes
// do, so no scan of every node is crossed with them and filtered back down.
class UnwindCollectSeedTest : public TuringTest {
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
        analyzer.setV3();
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        return owningModule;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        RowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        Rows actual = sink.rows();
        std::sort(actual.begin(), actual.end());

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

TEST_F(UnwindCollectSeedTest, aCollectedNodeOpensTheTraversal) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (p:Person) WITH collect(p) AS people UNWIND people AS person "
                 "MATCH (person)-[:INTERESTED_IN]->(i) RETURN i.name");

    // The unwound nodes are the hop's input, so the second MATCH crosses nothing and adds
    // no scan of its own: the one left is the labelled scan the collect gathered.
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::Unwind>(*module), 1u);
}

TEST_F(UnwindCollectSeedTest, theUnwoundNodeIsTheHopsInput) {
    mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (p:Person) WITH collect(p) AS people UNWIND people AS person "
                 "MATCH (person)-[:INTERESTED_IN]->(i) RETURN i.name");

    mlir::db::Unwind unwind;
    module->walk([&unwind](mlir::db::Unwind found) {
        unwind = found;
    });
    ASSERT_TRUE(unwind);

    mlir::db::GetOutEdgesByType hop;
    module->walk([&hop](mlir::db::GetOutEdgesByType found) {
        hop = found;
    });
    ASSERT_TRUE(hop);

    EXPECT_EQ(hop.getInputNodes(), unwind.getElement());
}

// A pattern reaching the unwound variable from its other end is walked backwards from it,
// so it opens the traversal just the same.
TEST_F(UnwindCollectSeedTest, aCollectedNodeOpensTheTraversalAsThePatternTarget) {
    const mlir::OwningOpRef<mlir::ModuleOp> module =
        generate("MATCH (p:Interest) WITH collect(p) AS interests UNWIND interests AS interest "
                 "MATCH (n)-[:INTERESTED_IN]->(interest) RETURN n.name");

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}

TEST_F(UnwindCollectSeedTest, expandsEachCollectedNodeOnce) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' WITH collect(p) AS people "
               "UNWIND people AS person MATCH (person)-[:INTERESTED_IN]->(i) RETURN i.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(UnwindCollectSeedTest, projectsTheUnwoundNodeBesideWhatItReached) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' WITH collect(p) AS people "
               "UNWIND people AS person MATCH (person)-[:INTERESTED_IN]->(i) "
               "RETURN person.name, i.name",
               {{"Adam", "Bio"}, {"Adam", "Cooking"}});
}

TEST_F(UnwindCollectSeedTest, expandsAnEmptyCollectToNoRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'nobody' WITH collect(p) AS people "
               "UNWIND people AS person MATCH (person)-[:INTERESTED_IN]->(i) RETURN i.name",
               {});
}
