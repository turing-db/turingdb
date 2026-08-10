#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
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
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ConstantNameRow = std::pair<int64_t, std::string>;
using ConstantNameRows = std::vector<ConstantNameRow>;

// Collects a projection that pairs a constant with a name, in the order the sink sees
// them. The constant is read through ColumnConst, which answers the same value at every
// subscript, so a row it is asked for is a row that exists only if the name column has
// it.
class ConstantNameSink : public NLOutputSink {
public:
    ConstantNameSink(size_t constantColumn, size_t nameColumn)
        : _constantColumn(constantColumn),
          _nameColumn(nameColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks[_constantColumn]);
        ASSERT_NE(constants, nullptr);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[_nameColumn]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        ASSERT_LE(offset + rowCount, nameRaw.size());

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back((*constants)[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    const ConstantNameRows& rows() const { return _rows; }

private:
    ConstantNameRows _rows;
    size_t _constantColumn {0};
    size_t _nameColumn {0};
};

}

// A constant column broadcasts: it holds one value, which stands for every row of the
// step it is emitted with. So it says nothing about how many rows that step has, and a
// projection mixing one with a per-row column has to take its row count from the per-row
// column - down to the step that kept no row at all, which the constant would otherwise
// make one row long.
class ConstantOutputTest : public TuringTest {
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

    void expectConstantNameRows(std::string_view query,
                                size_t constantColumn,
                                size_t nameColumn,
                                const ConstantNameRows& expected) {
        ConstantNameSink sink(constantColumn, nameColumn);
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// One node is named Remy, so the projection is that one row and the constant stands for
// it: the row count the two columns agree on is the name column's.
TEST_F(ConstantOutputTest, emitsTheMatchedRowBesideTheConstant) {
    const ConstantNameRows expected = {{5, "Remy"}};
    expectConstantNameRows("MATCH (n) WHERE n.name = 'Remy' RETURN 5, n.name", 0, 1, expected);
}

// No node carries that name, so the filter leaves the step with no row and the
// projection is empty. The constant still holds its one value - it is not a row, and
// counting it as one would answer a query that matched nothing with a row of whatever
// the empty name column reads back.
TEST_F(ConstantOutputTest, emitsNothingWhenTheMatchIsEmpty) {
    expectConstantNameRows("MATCH (n) WHERE n.name = 'nobody' RETURN 5, n.name", 0, 1, {});
}

// Which column the constant is changes nothing: the count comes from the column that
// carries rows, wherever it sits in the projection.
TEST_F(ConstantOutputTest, emitsNothingWhenTheMatchIsEmptyPastATrailingConstant) {
    expectConstantNameRows("MATCH (n) WHERE n.name = 'nobody' RETURN n.name, 5", 1, 0, {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
