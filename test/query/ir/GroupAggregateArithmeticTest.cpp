#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (nullable int64 grouping key, uint64 value) rows a grouped aggregate whose
// value is arithmetic over a count emits: a nullable i64 key chunk and a non-null ui64
// value chunk. 2 * count(n) + 20 and count(n) + 1 both promote to a non-null ui64 because
// count is a ui64 and every other operand is a non-null integer literal.
class GroupIntValueSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<int64_t>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(keys->size(), values->size());

        const auto& keyRaw = keys->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<int64_t> key;
            if (keyRaw[rowIndex]) {
                key = *keyRaw[rowIndex];
            }

            _rows.push_back({key, valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

}

// A grouped aggregate whose projection item wraps the aggregate in arithmetic: the group
// reduces the count and the surrounding 2 * ... + 20 is computed over the per-group result,
// the grouped analogue of the scalar RETURN 2 * count(n) + 20 that already works.
class GroupAggregateArithmeticTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// simpledb has eight Person nodes; only Remy and Adam carry an age (32), so grouping on
// n.age forms two groups: age 32 with count 2, and the null-age group with count 6.
// 2 * count + 20 is therefore 24 for the 32 group and 32 for the null-age group.
TEST_F(GroupAggregateArithmeticTest, scalesAndOffsetsACountPerGroup) {
    GroupIntValueSink sink;
    runQuery("MATCH (n:Person) RETURN n.age, 2 * count(n) + 20", &sink);

    std::vector<GroupIntValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<GroupIntValueSink::Row> expected {{std::nullopt, 32}, {32, 24}};
    EXPECT_EQ(rows, expected);
}

// The same grouping through a lighter expression: count + 1 is 3 for the age-32 group
// (count 2) and 7 for the null-age group (count 6).
TEST_F(GroupAggregateArithmeticTest, incrementsACountPerGroup) {
    GroupIntValueSink sink;
    runQuery("MATCH (n:Person) RETURN n.age, count(n) + 1", &sink);

    std::vector<GroupIntValueSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<GroupIntValueSink::Row> expected {{std::nullopt, 7}, {32, 3}};
    EXPECT_EQ(rows, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
