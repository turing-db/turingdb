#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/Column.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Counts the rows whose target column holds a null (nullopt) optional value.
class NullSink : public NLOutputSink {
public:
    explicit NullSink(size_t nullColumn = 0)
        : _nullColumn(nullColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_LT(_nullColumn, chunks.size());

        const ColumnOptVector<types::Int64::Primitive>* nullColumn =
            dynamic_cast<const ColumnOptVector<types::Int64::Primitive>*>(chunks[_nullColumn]);
        ASSERT_NE(nullColumn, nullptr);

        for (size_t row = offset; row < offset + rowCount; ++row) {
            if (!(*nullColumn)[row].has_value()) {
                ++_nullRows;
            }
        }

        _totalRows += rowCount;
    }

    size_t getNullRows() const { return _nullRows; }
    size_t getTotalRows() const { return _totalRows; }

private:
    size_t _nullRows {0};
    size_t _totalRows {0};
    size_t _nullColumn {0};
};

}

class FunctionInputColumnTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink* sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(FunctionInputColumnTest, toIntegerOverLabelsReachesFunctor) {
    NullSink sink;
    QueryStatus status;

    runQuery("MATCH (n) RETURN toInteger(labels(n))", status, &sink);

    EXPECT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getTotalRows(), 18u);
    EXPECT_EQ(sink.getNullRows(), 18u);
}

// DISABLED: analyzer signature does not yet accept NULL
TEST_F(FunctionInputColumnTest, DISABLED_toIntegerOfNullYieldsNullConstant) {
    NullSink sink(1);
    QueryStatus status;
    runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN n.name, toInteger(null)", status, &sink);

    EXPECT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getTotalRows(), 1u);
    EXPECT_EQ(sink.getNullRows(), 1u);
}
