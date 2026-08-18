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
#include "columns/ColumnConst.h"
#include "metadata/PropertyNull.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Records how many of the rows it sees carry a null constant in the target column.
class NullConstantSink : public NLOutputSink {
public:
    explicit NullConstantSink(size_t nullColumn)
        : _nullColumn(nullColumn)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t, size_t rowCount) override {
        ASSERT_LT(_nullColumn, chunks.size());

        const bool isNullConstant = dynamic_cast<const ColumnConst<PropertyNull>*>(chunks[_nullColumn]) != nullptr;
        if (isNullConstant) {
            _nullConstantRows += rowCount;
        }

        _totalRows += rowCount;
    }

    size_t getNullConstantRows() const { return _nullConstantRows; }
    size_t getTotalRows() const { return _totalRows; }

private:
    size_t _nullConstantRows {0};
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

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_NE(status.getError().find("cannot convert"), std::string::npos) << status.getError();
    EXPECT_EQ(status.getError().find("unexpected column type"), std::string::npos) << status.getError();
}

TEST_F(FunctionInputColumnTest, toIntegerOfNullYieldsNullConstant) {
    NullConstantSink sink(1);
    QueryStatus status;
    runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN n.name, toInteger(null)", status, &sink);

    EXPECT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getTotalRows(), 1u);
    EXPECT_EQ(sink.getNullConstantRows(), 1u);
}
