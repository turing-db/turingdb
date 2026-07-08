#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <span>
#include <stdexcept>
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
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<uint64_t>;
using Rows = std::vector<Row>;

uint64_t readID(const Column* column, size_t row) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return (*nodeIDs)[row].getValue();
    } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        return (*edgeIDs)[row].getValue();
    } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
        return (*edgeTypes)[row].getValue();
    }

    throw std::runtime_error("CypherOutputTest: unsupported output column type");
}

void describeRows(const Rows& rows, std::string& out) {
    out.clear();
    for (const Row& row : rows) {
        out += "        {";
        for (size_t index = 0; index < row.size(); index++) {
            if (index > 0) {
                out += ", ";
            }

            out += std::to_string(row[index]);
        }

        out += "},\n";
    }
}

class CollectingIDSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        if (_columns.empty()) {
            _columns.resize(chunks.size());
        }

        ASSERT_EQ(chunks.size(), _columns.size());

        for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
            const Column* column = chunks[columnIndex];

            for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
                _columns[columnIndex].push_back(readID(column, rowIndex));
            }
        }
    }

    void sortedRows(Rows& rows) const {
        rows.clear();
        const size_t rowCount = _columns.empty() ? 0 : _columns.front().size();

        for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            Row& row = rows.emplace_back();
            for (const std::vector<uint64_t>& column : _columns) {
                row.push_back(column[rowIndex]);
            }
        }

        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::vector<uint64_t>> _columns;
};

}

class CypherOutputTest : public TuringTest {
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
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        CollectingIDSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        interpreter.run();

        sink.sortedRows(rows);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        runQuery(query, actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string description;
        describeRows(actual, description);

        EXPECT_EQ(actual, sortedExpected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CypherOutputTest, scanNodes) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectRows("MATCH (n) RETURN n", expected);
}

TEST_F(CypherOutputTest, outEdgesSource) {
    const Rows expected = {
        {0}, {0}, {0}, {0}, {1}, {1}, {1}, {6}, {8}, {8},
        {9}, {9}, {11}, {12}, {12}, {15}, {15}, {17},
    };
    expectRows("MATCH (a)-->(b) RETURN a", expected);
}

TEST_F(CypherOutputTest, outEdgesTarget) {
    const Rows expected = {
        {0}, {0}, {1}, {2}, {2}, {3}, {4}, {4}, {5}, {5},
        {6}, {7}, {10}, {13}, {13}, {13}, {14}, {16},
    };
    expectRows("MATCH (a)-->(b) RETURN b", expected);
}

TEST_F(CypherOutputTest, outEdgesPair) {
    const Rows expected = {
        {0, 1}, {0, 2}, {0, 3}, {0, 6}, {1, 0}, {1, 4}, {1, 5}, {6, 0},
        {8, 4}, {8, 7}, {9, 2}, {9, 10}, {11, 5}, {12, 13}, {12, 16},
        {15, 13}, {15, 14}, {17, 13},
    };
    expectRows("MATCH (a)-->(b) RETURN a, b", expected);
}

TEST_F(CypherOutputTest, inEdgesPair) {
    const Rows expected = {
        {0, 1}, {0, 6}, {1, 0}, {2, 0}, {2, 9}, {3, 0}, {4, 1}, {4, 8},
        {5, 1}, {5, 11}, {6, 0}, {7, 8}, {10, 9}, {13, 12}, {13, 15},
        {13, 17}, {14, 15}, {16, 12},
    };
    expectRows("MATCH (a)<--(b) RETURN a, b", expected);
}

TEST_F(CypherOutputTest, twoHop) {
    const Rows expected = {
        {0, 0}, {0, 0}, {0, 4}, {0, 5}, {1, 1}, {1, 2}, {1, 3}, {1, 6},
        {6, 1}, {6, 2}, {6, 3}, {6, 6},
    };
    expectRows("MATCH (a)-->(b)-->(c) RETURN a, c", expected);
}

TEST_F(CypherOutputTest, divergingTree) {
    const Rows expected = {
        {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0},
        {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1},
        {6},
        {8}, {8}, {8}, {8},
        {9}, {9}, {9}, {9},
        {11},
        {12}, {12}, {12}, {12},
        {15}, {15}, {15}, {15},
        {17},
    };
    expectRows("MATCH (a)-->(b), (a)-->(c) RETURN a", expected);
}

TEST_F(CypherOutputTest, convergingReturnShared) {
    const Rows expected = {
        {0}, {0}, {0}, {0},
        {1},
        {2}, {2}, {2}, {2},
        {3},
        {4}, {4}, {4}, {4},
        {5}, {5}, {5}, {5},
        {6},
        {7},
        {10},
        {13}, {13}, {13}, {13}, {13}, {13}, {13}, {13}, {13},
        {14},
        {16},
    };
    expectRows("MATCH (b)-->(a), (c)-->(a) RETURN a", expected);
}

TEST_F(CypherOutputTest, convergingReturnSecondSource) {
    const Rows expected = {
        {0}, {0}, {0}, {0}, {0},
        {1}, {1}, {1}, {1}, {1}, {1},
        {6}, {6},
        {8}, {8}, {8},
        {9}, {9}, {9},
        {11}, {11},
        {12}, {12}, {12}, {12},
        {15}, {15}, {15}, {15},
        {17}, {17}, {17},
    };
    expectRows("MATCH (b)-->(a), (c)-->(a) RETURN c", expected);
}

TEST_F(CypherOutputTest, chainThenConverge) {
    const Rows expected = {
        {0}, {0}, {0}, {0},
        {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1},
        {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6},
    };
    expectRows("MATCH (a)-->(b)-->(c), (d)-->(b) RETURN a", expected);
}

TEST_F(CypherOutputTest, chainThenDiverge) {
    const Rows expected = {
        {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0},
        {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1},
        {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6}, {6},
    };
    expectRows("MATCH (a)-->(b)-->(c), (b)-->(e) RETURN a", expected);
}
