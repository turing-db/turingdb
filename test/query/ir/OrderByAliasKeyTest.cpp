#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
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

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using OptInt64Values = std::vector<std::optional<int64_t>>;

// The eighteen edges of simpledb by duration, ascending. Eight carry one - 10, 15, five
// times 20 and 200 - and the ten others have none; a null sorts after every value, so
// they close the order. The five rows sharing 20 tie, and a tie holds no expectation of
// its own: they are equal, whichever traversal order they were collected in.
const OptInt64Values edgeDurationsAscending = {
    10, 15, 20, 20, 20, 20, 20, 200,
    std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
    std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
};

// The same order reversed, which moves the nulls to the front since they sort after
// every value.
const OptInt64Values edgeDurationsDescending = {
    std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
    std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
    200, 20, 20, 20, 20, 20, 15, 10,
};

// Collects the single projected nullable integer column, in the order the sink sees it.
// An ORDER BY is only correct if that order survives to the output, so nothing here
// sorts what it collected.
class OrderedOptInt64Sink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(valueRaw[rowIndex]);
        }
    }

    const OptInt64Values& values() const { return _values; }

private:
    OptInt64Values _values;
};

}

// An ORDER BY key may name the alias a return item was given, and an alias is only
// another spelling of that item: a key naming one must order the rows exactly as the
// item spelled out again does, wherever the alias sits in the key. Spelled out, the
// key e.duration + 1 is already a key of its own, computed into a column and appended
// to the sort - naming it edgeDuration + 1 asks for the same column.
class OrderByAliasKeyTest : public TuringTest {
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

    void expectDurations(std::string_view query, const OptInt64Values& expected) {
        OrderedOptInt64Sink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The key spells the item out, so nothing has to be matched to an alias: this is the
// order the two cases below have to reproduce.
TEST_F(OrderByAliasKeyTest, spelledOutCompoundKeyOrders) {
    expectDurations("MATCH (a)-[e]->(b) RETURN e.duration AS edgeDuration ORDER BY e.duration + 1",
                    edgeDurationsAscending);
}

TEST_F(OrderByAliasKeyTest, spelledOutCompoundKeyOrdersDescending) {
    expectDurations("MATCH (a)-[e]->(b) RETURN e.duration AS edgeDuration ORDER BY e.duration + 1 DESC",
                    edgeDurationsDescending);
}

// The key is the alias alone, which names the projected column through the declaration
// the two share, so the sort keys on the column already there.
TEST_F(OrderByAliasKeyTest, bareAliasKeyOrders) {
    expectDurations("MATCH (a)-[e]->(b) RETURN e.duration AS edgeDuration ORDER BY edgeDuration",
                    edgeDurationsAscending);
}

// The alias inside a computed key. The key as a whole names no item, so it is computed
// into a column of its own - and computing it means reading the alias, which stands for
// the item it was given to.
TEST_F(OrderByAliasKeyTest, aliasInsideACompoundKeyOrdersLikeTheItem) {
    expectDurations("MATCH (a)-[e]->(b) RETURN e.duration AS edgeDuration ORDER BY edgeDuration + 1",
                    edgeDurationsAscending);
}

// A direction on the key changes nothing about what the alias in it names.
TEST_F(OrderByAliasKeyTest, aliasInsideACompoundKeyOrdersDescending) {
    expectDurations("MATCH (a)-[e]->(b) RETURN e.duration AS edgeDuration ORDER BY edgeDuration + 1 DESC",
                    edgeDurationsDescending);
}

// A DISTINCT drops rows before the sort sees them, so a key computed from an item would
// be one value per row the dedup was given rather than per row it kept. The analyzer
// turns those keys away, which is what leaves the column of an item safe to key on.
TEST_F(OrderByAliasKeyTest, aliasInsideACompoundKeyIsRejectedUnderDistinct) {
    const std::string_view query =
        "MATCH (a)-[e]->(b) RETURN DISTINCT e.duration AS edgeDuration ORDER BY edgeDuration + 1";

    OrderedOptInt64Sink sink;
    EXPECT_THROW(runQuery(query, &sink), AnalyzeException);
}
