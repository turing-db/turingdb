#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <range/v3/view/zip.hpp>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

constexpr std::string_view unrenderedCell = "<unrendered column>";

template <typename T>
void renderValue(const T& value, std::string& text) {
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
        text = value;
    } else if constexpr (std::is_same_v<T, ChangeID>) {
        text = std::to_string(value.get());
    } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
        text = static_cast<bool>(value) ? "true" : "false";
    } else {
        text = std::to_string(value);
    }
}

// One cell, rendered if the column is a vector of the spelled element type.
template <typename T>
bool renderIfVector(const Column* column, size_t row, std::string& text) {
    if (column->getKind() != ColumnVector<T>::staticKind()) {
        return false;
    }

    const ColumnVector<T>* const values = static_cast<const ColumnVector<T>*>(column);
    renderValue((*values)[row], text);

    return true;
}

// The same cell when the column carries one repeated value instead of a vector. The
// two paths pick their own column for a single-row report - the pipeline hands back
// a constant where the MLIR engine fills a one-row vector - so the value is what
// compares, not the column it arrived in.
template <typename T>
bool renderIfConst(const Column* column, size_t row, std::string& text) {
    if (column->getKind() != ColumnConst<T>::staticKind()) {
        return false;
    }

    const ColumnConst<T>* const values = static_cast<const ColumnConst<T>*>(column);
    renderValue(values->at(row), text);

    return true;
}

template <typename T>
bool renderIfKind(const Column* column, size_t row, std::string& text) {
    return renderIfVector<T>(column, row, text) || renderIfConst<T>(column, row, text);
}

void renderCell(const Column* column, size_t row, std::string& text) {
    const bool rendered = renderIfKind<types::String::Primitive>(column, row, text)
                       || renderIfKind<std::string>(column, row, text)
                       || renderIfKind<types::UInt64::Primitive>(column, row, text)
                       || renderIfKind<types::Bool::Primitive>(column, row, text)
                       || renderIfKind<ChangeID>(column, row, text);

    if (!rendered) {
        text = unrenderedCell;
    }
}

// The result table of a command run through the MLIR engine.
class SystemTableSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t row = offset; row < offset + rowCount; row++) {
            Row& cells = _rows.emplace_back();
            cells.resize(chunks.size());

            for (size_t column = 0; column < chunks.size(); column++) {
                renderCell(chunks[column], row, cells[column]);
            }
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const Rows& getRows() const { return _rows; }

private:
    std::vector<std::string> _columnNames;
    Rows _rows;
};

}

// The two engines answer the same statements, and the server serialises whatever
// table the one it runs hands back - so a command's column names, its column types
// and its rows have to agree across them. The pipeline is what the server runs
// today, which makes it the reference the MLIR path has to match.
class SystemCommandEquivalenceTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _db = &_env->getDB();
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runPipeline(std::string_view query,
                     std::vector<std::string>& columnNames,
                     Rows& rows) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&columnNames, &rows](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe != nullptr);

            const Dataframe::NamedColumns& columns = dataframe->cols();
            const size_t rowCount = dataframe->getLogicalRowCount();

            columnNames.clear();
            for (const NamedColumn* column : columns) {
                columnNames.emplace_back(column->getName());
            }

            for (size_t row = 0; row < rowCount; row++) {
                Row& cells = rows.emplace_back();
                cells.resize(columns.size());

                for (size_t column = 0; column < columns.size(); column++) {
                    renderCell(columns[column]->getColumn(), row, cells[column]);
                }
            }
        });

        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);
        ASSERT_TRUE(status.isOk()) << "the pipeline rejected '" << query << "'";
    }

    void runMLIR(std::string_view query, SystemTableSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "the MLIR engine rejected '" << query << "': "
                                   << status.getError();
    }

    // A column type the renderer does not know renders to the same placeholder on
    // both sides, which would compare equal and say nothing.
    void expectEveryCellRendered(const Rows& rows, std::string_view query) {
        for (const Row& row : rows) {
            for (const std::string& cell : row) {
                EXPECT_NE(cell, unrenderedCell) << "'" << query << "' reports a column the test "
                                                   "cannot render, so the comparison is empty";
            }
        }
    }

    // A command that reports the same table however it is run: same column names,
    // same rows. The order is not part of the contract, so both are sorted.
    void expectSameTable(std::string_view query) {
        std::vector<std::string> pipelineNames;
        Rows pipelineRows;
        runPipeline(query, pipelineNames, pipelineRows);

        SystemTableSink sink;
        runMLIR(query, sink);

        EXPECT_EQ(pipelineNames, sink.getColumnNames()) << "column names differ for '" << query << "'";

        expectEveryCellRendered(pipelineRows, query);
        expectEveryCellRendered(sink.getRows(), query);

        Rows mlirRows = sink.getRows();
        std::ranges::sort(pipelineRows);
        std::ranges::sort(mlirRows);

        ASSERT_EQ(pipelineRows.size(), mlirRows.size()) << "row count differs for '" << query << "'";

        for (auto [pipelineRow, mlirRow] : rv::zip(pipelineRows, mlirRows)) {
            EXPECT_EQ(pipelineRow, mlirRow) << "row differs for '" << query << "'";
        }
    }

    // A command whose report carries something it just made - an ID, a name the two
    // runs cannot share - so only the shape is comparable.
    void expectSameShape(std::string_view pipelineQuery, std::string_view mlirQuery) {
        std::vector<std::string> pipelineNames;
        Rows pipelineRows;
        runPipeline(pipelineQuery, pipelineNames, pipelineRows);

        SystemTableSink sink;
        runMLIR(mlirQuery, sink);

        EXPECT_EQ(pipelineNames, sink.getColumnNames());
        EXPECT_EQ(pipelineRows.size(), sink.getRows().size());

        expectEveryCellRendered(pipelineRows, pipelineQuery);
        expectEveryCellRendered(sink.getRows(), mlirQuery);

        for (auto [pipelineRow, mlirRow] : rv::zip(pipelineRows, sink.getRows())) {
            EXPECT_EQ(pipelineRow.size(), mlirRow.size());
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(SystemCommandEquivalenceTest, listGraph) {
    expectSameTable("LIST GRAPH");
}

TEST_F(SystemCommandEquivalenceTest, listAvailableGraphs) {
    expectSameTable("LIST AVAILABLE GRAPHS");
}

TEST_F(SystemCommandEquivalenceTest, showProcedures) {
    expectSameTable("SHOW PROCEDURES");
}

TEST_F(SystemCommandEquivalenceTest, showExtensions) {
    expectSameTable("SHOW EXTENSIONS");
}

TEST_F(SystemCommandEquivalenceTest, showVectorIndexes) {
    expectSameTable("SHOW VECTOR INDEXES");
}

// A vector index the two runs share would collide, so each makes its own and the
// listing that follows is what has to agree - both engines see one index either way.
TEST_F(SystemCommandEquivalenceTest, createVectorIndex) {
    expectSameShape("CREATE VECTOR INDEX pipelineVectors WITH DIMENSION 4 METRIC EUCLID",
                    "CREATE VECTOR INDEX mlirVectors WITH DIMENSION 4 METRIC EUCLID");

    expectSameTable("SHOW VECTOR INDEXES");
}

TEST_F(SystemCommandEquivalenceTest, deleteVectorIndex) {
    expectSameShape("CREATE VECTOR INDEX pipelineVectors WITH DIMENSION 4 METRIC EUCLID",
                    "CREATE VECTOR INDEX mlirVectors WITH DIMENSION 4 METRIC EUCLID");

    expectSameShape("DELETE VECTOR INDEX pipelineVectors", "DELETE VECTOR INDEX mlirVectors");

    expectSameTable("SHOW VECTOR INDEXES");
}

// Each run opens its own change, so the IDs differ by construction.
TEST_F(SystemCommandEquivalenceTest, changeNew) {
    expectSameShape("CHANGE NEW", "CHANGE NEW");
}

TEST_F(SystemCommandEquivalenceTest, changeList) {
    expectSameShape("CHANGE NEW", "CHANGE NEW");

    expectSameTable("CHANGE LIST");
}
