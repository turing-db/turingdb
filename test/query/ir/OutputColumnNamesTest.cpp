#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/Column.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ColumnNames = std::vector<std::string>;

// Records the names the program published and the width of the chunks it then emitted,
// so a test can hold the names against the columns they label.
class ColumnNameSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _names.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _emittedColumnCount = chunks.size();
        _rowCount += rowCount;
    }

    const ColumnNames& names() const { return _names; }
    size_t getEmittedColumnCount() const { return _emittedColumnCount; }
    size_t getRowCount() const { return _rowCount; }

private:
    ColumnNames _names;
    size_t _emittedColumnCount {0};
    size_t _rowCount {0};
};

}

// A RETURN item's name - the alias it was given through AS, or the item spelled as it was
// written - reaches the sink, which has no other way to label the columns it is fed: the
// names ride the projection from db.output through the lowering to nl.output, and the
// interpreter hands them over once before the first chunk.
class OutputColumnNamesTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
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

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, sink, &memory);
        interpreter.run();
    }

    void expectColumnNames(std::string_view query, const ColumnNames& expected) {
        ColumnNameSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.names(), expected) << "query: " << query;

        // A name labels a column the sink was handed, so a query that emitted rows names
        // exactly as many columns as it emitted.
        if (sink.getRowCount() > 0) {
            EXPECT_EQ(sink.names().size(), sink.getEmittedColumnCount()) << "query: " << query;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(OutputColumnNamesTest, namesAnAliasedProperty) {
    expectColumnNames("MATCH (n:Person) RETURN n.name AS name", {"name"});
}

// An item without an alias is still named - it keeps the spelling it was written with.
TEST_F(OutputColumnNamesTest, namesAnUnaliasedItemAsItWasWritten) {
    expectColumnNames("MATCH (n:Person) RETURN n.name, n.age AS age", {"n.name", "age"});
}

TEST_F(OutputColumnNamesTest, namesAnAliasedTraversalVariable) {
    expectColumnNames("MATCH (n:Person) RETURN n AS person", {"person"});
}

TEST_F(OutputColumnNamesTest, namesAnAliasedAggregate) {
    expectColumnNames("MATCH (n:Person) RETURN count(n) AS total", {"total"});
}

// A wildcard names no item itself; the variables it stands for carry their own names.
TEST_F(OutputColumnNamesTest, namesTheVariablesAWildcardStandsFor) {
    expectColumnNames("MATCH (n:Person) RETURN *", {"n"});
}

// A constant projection has no per-row column, so its output is emitted nested in the
// driving loop with a cardinality operand. The names travel with it all the same.
TEST_F(OutputColumnNamesTest, namesAnAliasedConstant) {
    expectColumnNames("MATCH (n:Person) RETURN 5 AS five", {"five"});
}

TEST_F(OutputColumnNamesTest, namesPastADistinct) {
    expectColumnNames("MATCH (n:Person) RETURN DISTINCT n.age AS age", {"age"});
}

TEST_F(OutputColumnNamesTest, namesPastASortOnTheAlias) {
    expectColumnNames("MATCH (n:Person) RETURN n.name AS name ORDER BY name", {"name"});
}

// A terminal LIMIT folds into the output, which is re-emitted over the untruncated
// columns: the fold has to carry the names across, or the query that asks for a prefix
// loses them.
TEST_F(OutputColumnNamesTest, namesPastALimitFoldedIntoTheOutput) {
    expectColumnNames("MATCH (n:Person) RETURN n.name AS name LIMIT 2", {"name"});
}

// The skip sibling of the limit fold, re-emitting the output over the surviving suffix.
TEST_F(OutputColumnNamesTest, namesPastASkipFoldedIntoTheOutput) {
    expectColumnNames("MATCH (n:Person) RETURN n.name AS name SKIP 6", {"name"});
}

TEST_F(OutputColumnNamesTest, namesEveryColumnOfAWiderProjection) {
    expectColumnNames("MATCH (n:Person) RETURN n AS person, n.name AS name, n.age AS age",
                      {"person", "name", "age"});
}

// A query matching nothing emits no chunk, yet the result still has named columns: the
// names come off the program, not off a row.
TEST_F(OutputColumnNamesTest, namesTheColumnsOfAnEmptyResult) {
    ColumnNameSink sink;
    runQuery("MATCH (n:Person) WHERE n.name = 'nobody' RETURN n.name AS name", &sink);

    EXPECT_EQ(sink.getRowCount(), 0u);
    EXPECT_EQ(sink.names(), ColumnNames {"name"});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
