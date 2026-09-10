#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
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
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

template <typename T>
std::optional<std::string> renderOptCell(const Column* column, size_t row) {
    const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
    if (!values) {
        return std::nullopt;
    }

    const std::optional<T>& value = (*values)[row];
    if (!value) {
        return "null";
    }

    if constexpr (std::is_same_v<T, CustomBool>) {
        return static_cast<bool>(*value) ? "true" : "false";
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string(*value);
    } else {
        return std::to_string(*value);
    }
}

std::string renderTaggedElement(const ListElementView element);

std::string renderTaggedList(const ListView list) {
    std::string rendered = "[";
    for (size_t index = 0; const ListElementView element : list) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += renderTaggedElement(element);
        index++;
    }

    return rendered + "]";
}

std::string renderTaggedElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::Double:
            return std::to_string(element.getAs<double>());
        break;

        case ListBufferTypeTag::Bool:
            return static_cast<bool>(element.getAs<CustomBool>()) ? "true" : "false";
        break;

        case ListBufferTypeTag::String:
            return std::string(element.getAs<std::string_view>());
        break;

        case ListBufferTypeTag::ListView:
            return renderTaggedList(element.getAs<ListView>());
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        default:
            return "?";
        break;
    }
}

std::optional<std::string> renderOptTaggedCell(const Column* column, size_t row) {
    const auto* elements = dynamic_cast<const ColumnOptVector<ListElementView>*>(column);
    if (!elements) {
        return std::nullopt;
    }

    const std::optional<ListElementView>& element = (*elements)[row];
    if (!element) {
        return "null";
    }

    return renderTaggedElement(*element);
}

std::string renderCell(const Column* column, size_t row) {
    if (const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(column)) {
        return std::to_string((*counts)[row]);
    }

    if (const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(column)) {
        return renderTaggedList((*lists)[row]);
    }

    if (const std::optional<std::string> integer = renderOptCell<int64_t>(column, row)) {
        return *integer;
    } else if (const std::optional<std::string> real = renderOptCell<double>(column, row)) {
        return *real;
    } else if (const std::optional<std::string> tagged = renderOptTaggedCell(column, row)) {
        return *tagged;
    }

    throw TuringException("ListIndexAggregateTest: unsupported output column type");
}

class CollectingRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(renderCell(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

class ListIndexAggregateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows) {
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

        CollectingRowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, &sink, &memory);
        interpreter.run();

        rows = sink.rows();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        runQuery(query, actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ListIndexAggregateTest, countsTheElementsOfEachGroup) {
    const Rows expected = {{"0", "2"}, {"1", "0"}, {"2", "1"}, {"5", "0"}};
    expectRows("UNWIND [0, 1, 2, 5, 0] AS i RETURN i, count([10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, countsTheDistinctElements) {
    const Rows expected = {{"2"}};
    expectRows("UNWIND [0, 0, 2, 1, 5] AS i RETURN count(DISTINCT [10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, sumsTheElements) {
    const Rows expected = {{std::to_string(40.0)}};
    expectRows("UNWIND [0, 1, 2, 5] AS i RETURN sum([10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, sumsTheElementsOfEachGroup) {
    const Rows expected = {{"0", std::to_string(20.0)},
                           {"1", "null"},
                           {"2", std::to_string(30.0)},
                           {"5", "null"}};
    expectRows("UNWIND [0, 1, 2, 5, 0] AS i RETURN i, sum([10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, averagesTheElements) {
    const Rows expected = {{std::to_string(20.0)}};
    expectRows("UNWIND [0, 1, 2, 5] AS i RETURN avg([10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, collectsTheElements) {
    const Rows expected = {{"[10, 30]"}};
    expectRows("UNWIND [0, 1, 2, 5] AS i RETURN collect([10, null, 30][i])", expected);
}

TEST_F(ListIndexAggregateTest, collectsTheDistinctElements) {
    const Rows expected = {{"[10, 30]"}};
    expectRows("UNWIND [0, 0, 2, 1, 5] AS i RETURN collect(DISTINCT [10, null, 30][i])", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
