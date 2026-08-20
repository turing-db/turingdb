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
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "TuringException.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads one cell of a numeric output column as a double, whatever integer/float width and
// nullability the arithmetic resolved to (count is ui64, a sum is a nullable i64, avg is a
// f64, and mixing them promotes), so a test can assert values without predicting the exact
// column type of every expression. Returns nullopt for a null cell.
std::optional<double> readNumeric(const Column* column, size_t row) {
    if (const auto* c = dynamic_cast<const ColumnVector<int64_t>*>(column)) {
        return static_cast<double>(c->getRaw()[row]);
    } else if (const auto* c = dynamic_cast<const ColumnVector<uint64_t>*>(column)) {
        return static_cast<double>(c->getRaw()[row]);
    } else if (const auto* c = dynamic_cast<const ColumnVector<double>*>(column)) {
        return c->getRaw()[row];
    } else if (const auto* c = dynamic_cast<const ColumnOptVector<int64_t>*>(column)) {
        const auto& value = c->getRaw()[row];
        return value ? std::optional<double>(static_cast<double>(*value)) : std::nullopt;
    } else if (const auto* c = dynamic_cast<const ColumnOptVector<uint64_t>*>(column)) {
        const auto& value = c->getRaw()[row];
        return value ? std::optional<double>(static_cast<double>(*value)) : std::nullopt;
    } else if (const auto* c = dynamic_cast<const ColumnOptVector<double>*>(column)) {
        const auto& value = c->getRaw()[row];
        return value ? std::optional<double>(*value) : std::nullopt;
    } else if (const auto* c = dynamic_cast<const ColumnConst<int64_t>*>(column)) {
        return static_cast<double>((*c)[row]);
    } else if (const auto* c = dynamic_cast<const ColumnConst<uint64_t>*>(column)) {
        return static_cast<double>((*c)[row]);
    } else if (const auto* c = dynamic_cast<const ColumnConst<double>*>(column)) {
        return (*c)[row];
    }

    ADD_FAILURE() << "unrecognized numeric column type";
    return std::nullopt;
}

std::optional<std::string> readStringKey(const Column* column, size_t row) {
    if (const auto* c = dynamic_cast<const ColumnOptVector<std::string_view>*>(column)) {
        const auto& value = c->getRaw()[row];
        return value ? std::optional<std::string>(std::string(*value)) : std::nullopt;
    } else if (const auto* c = dynamic_cast<const ColumnVector<std::string_view>*>(column)) {
        return std::string(c->getRaw()[row]);
    }

    ADD_FAILURE() << "unrecognized key column type";
    return std::nullopt;
}

// Collects the rows a grouped projection emits as (string key, one double-or-null per value
// column), so any RETURN n.team, <exprs...> can be asserted order-independently regardless
// of how many value columns it carries or what numeric type each resolved to.
class GroupExprSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<std::optional<double>>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_GE(chunks.size(), 2u);

        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key = readStringKey(chunks[0], row);

            std::vector<std::optional<double>> values;
            for (size_t column = 1; column < chunks.size(); column++) {
                values.push_back(readNumeric(chunks[column], row));
            }

            _rows.push_back({key, values});
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

// A matrix of grouped aggregations whose projection items combine aggregates and arithmetic
// every way round: several aggregates in one item, several items each with one, arithmetic
// inside an aggregate's argument, and nestings of both. Two teams with hand-derivable
// reductions (a null score inside "red" so per-group null handling is exercised) let each
// permutation assert exact per-group values, and the aggregate-in-aggregate cases assert the
// rejection of that invalid openCypher.
class GroupAggregateExpressionTest : public TuringTest {
protected:
    using Row = GroupExprSink::Row;

    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        buildTeamGraph(_graph);
    }

    // Five nodes over two teams. "red" holds scores 10, 20 and a third node with no score
    // (a null inside the group), with bonuses 1, 2, 3; "blue" holds scores 100, 50 with
    // bonuses 10, 20. So per team: count(n) is 3 / 2, count(n.score) is 2 / 2, sum(score)
    // is 30 / 150, min 10 / 50, max 20 / 100, avg 15.0 / 75.0, sum(bonus) 6 / 30.
    void buildTeamGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const NodeID red1 = writer.addNode({"N"});
        writer.addNodeProperty<types::String>(red1, "team", "red");
        writer.addNodeProperty<types::Int64>(red1, "score", 10);
        writer.addNodeProperty<types::Int64>(red1, "bonus", 1);

        const NodeID red2 = writer.addNode({"N"});
        writer.addNodeProperty<types::String>(red2, "team", "red");
        writer.addNodeProperty<types::Int64>(red2, "score", 20);
        writer.addNodeProperty<types::Int64>(red2, "bonus", 2);

        const NodeID red3 = writer.addNode({"N"});
        writer.addNodeProperty<types::String>(red3, "team", "red");
        writer.addNodeProperty<types::Int64>(red3, "bonus", 3);

        const NodeID blue1 = writer.addNode({"N"});
        writer.addNodeProperty<types::String>(blue1, "team", "blue");
        writer.addNodeProperty<types::Int64>(blue1, "score", 100);
        writer.addNodeProperty<types::Int64>(blue1, "bonus", 10);

        const NodeID blue2 = writer.addNode({"N"});
        writer.addNodeProperty<types::String>(blue2, "team", "blue");
        writer.addNodeProperty<types::Int64>(blue2, "score", 50);
        writer.addNodeProperty<types::Int64>(blue2, "bonus", 20);

        writer.submit();

        jobSystem.terminate();
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

    void expectRows(std::string_view query, const std::vector<Row>& expected) {
        GroupExprSink sink;
        runQuery(query, &sink);

        std::vector<Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectRejected(std::string_view query) {
        GroupExprSink sink;
        EXPECT_THROW(runQuery(query, &sink), TuringException) << "query: " << query;
    }

    static Row row(std::string key, std::vector<std::optional<double>> values) {
        return Row {std::optional<std::string>(std::move(key)), std::move(values)};
    }

    const std::string _graphName = "teams";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// Baseline: the grouped count these permutations build on, with no arithmetic around it.
TEST_F(GroupAggregateExpressionTest, groupedCountBaseline) {
    expectRows("MATCH (n) RETURN n.team, count(n)", {row("blue", {2}), row("red", {3})});
}

// One aggregate wrapped in arithmetic across several separate items.
TEST_F(GroupAggregateExpressionTest, oneAggregatePerItemAcrossItems) {
    expectRows("MATCH (n) RETURN n.team, count(n) + 1, sum(n.score) * 2",
               {row("blue", {3, 300}), row("red", {4, 60})});
}

// Several aggregates combined inside a single item.
TEST_F(GroupAggregateExpressionTest, multipleAggregatesInOneItem) {
    expectRows("MATCH (n) RETURN n.team, count(n) + sum(n.score)",
               {row("blue", {152}), row("red", {33})});

    expectRows("MATCH (n) RETURN n.team, sum(n.score) + min(n.score) + max(n.score)",
               {row("blue", {300}), row("red", {60})});
}

// Several items, each mixing aggregates and arithmetic differently.
TEST_F(GroupAggregateExpressionTest, multipleAggregatesAcrossMultipleItems) {
    expectRows("MATCH (n) RETURN n.team, sum(n.score) * 2, max(n.score) - min(n.score), count(n)",
               {row("blue", {300, 50, 2}), row("red", {60, 10, 3})});
}

// The aggregate's argument is itself an expression, reduced per group.
TEST_F(GroupAggregateExpressionTest, expressionInsideAnAggregate) {
    expectRows("MATCH (n) RETURN n.team, sum(n.score * 2)",
               {row("blue", {300}), row("red", {60})});

    expectRows("MATCH (n) RETURN n.team, sum(n.score + n.bonus)",
               {row("blue", {180}), row("red", {33})});

    expectRows("MATCH (n) RETURN n.team, max(n.score + 5), min(n.score + 5)",
               {row("blue", {105, 55}), row("red", {25, 15})});
}

// count over an expression counts only its non-null values: red's null score drops out.
TEST_F(GroupAggregateExpressionTest, countOverAnExpressionIgnoresNulls) {
    expectRows("MATCH (n) RETURN n.team, count(n.score + n.bonus)",
               {row("blue", {2}), row("red", {2})});
}

// Expression-in-aggregate and multiple aggregates combined in the same projection.
TEST_F(GroupAggregateExpressionTest, expressionInsideAggregateBesideOtherAggregates) {
    expectRows("MATCH (n) RETURN n.team, sum(n.score * 2) + count(n.score), max(n.score) - min(n.score)",
               {row("blue", {302, 50}), row("red", {62, 10})});
}

// Aggregates buried inside a compound arithmetic expression.
TEST_F(GroupAggregateExpressionTest, aggregatesInsideCompoundArithmetic) {
    expectRows("MATCH (n) RETURN n.team, (count(n) + sum(n.score)) * 2",
               {row("blue", {304}), row("red", {66})});

    expectRows("MATCH (n) RETURN n.team, sum(n.score) + sum(n.bonus) * 2",
               {row("blue", {210}), row("red", {42})});
}

// The same aggregate appearing twice in one expression.
TEST_F(GroupAggregateExpressionTest, sameAggregateTwiceInOneExpression) {
    expectRows("MATCH (n) RETURN n.team, count(n) * count(n)",
               {row("blue", {4}), row("red", {9})});
}

// A floating-point aggregate (avg) carried through arithmetic.
TEST_F(GroupAggregateExpressionTest, floatingPointAggregateInArithmetic) {
    expectRows("MATCH (n) RETURN n.team, avg(n.score) + 0.5",
               {row("blue", {75.5}), row("red", {15.5})});
}

// Every permutation at once in one item: three distinct aggregates, two of them over an
// expression, folded into an outer arithmetic expression. red is 60 + 25 * 1 = 85; blue is
// 300 + 105 * 10 = 1350.
TEST_F(GroupAggregateExpressionTest, deepPermutationInOneItem) {
    expectRows("MATCH (n) RETURN n.team, sum(n.score * 2) + max(n.score + 5) * min(n.bonus)",
               {row("blue", {1350}), row("red", {85})});
}

// Aggregate-in-aggregate is invalid openCypher and must be turned away, grouped or scalar.
TEST_F(GroupAggregateExpressionTest, rejectsAggregateInsideAggregate) {
    expectRejected("MATCH (n) RETURN n.team, count(sum(n.score))");
    expectRejected("MATCH (n) RETURN n.team, sum(count(n))");
    expectRejected("MATCH (n) RETURN n.team, sum(n.score + count(n))");
    expectRejected("MATCH (n) RETURN count(sum(n.score))");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
