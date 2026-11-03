#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// coalesce answers each row that its first argument leaves null with that row's own second
// argument. An unsigned integer has no literal, so simpledb gains four items carrying an
// unsigned property on two of them, one past the largest signed integer, and a signed
// fallback on all four.
class CoalesceUnsignedArgumentTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
        addItems(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void addItem(GraphWriter& writer, std::string_view name, const uint64_t* unsignedValue, int64_t fallback) {
        const NodeID item = writer.addNode({"Item"});
        writer.addNodeProperty<types::String>(item, "name", std::string_view {name});
        writer.addNodeProperty<types::Int64>(item, "fallback", int64_t {fallback});

        if (unsignedValue) {
            writer.addNodeProperty<types::UInt64>(item, "unsigned", uint64_t {*unsignedValue});
        }
    }

    void addItems(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        const uint64_t pastTheLargestSigned = 9223372036854775813ULL;
        const uint64_t seven = 7;

        GraphWriter writer(graph, &jobSystem);
        addItem(writer, "a", &pastTheLargestSigned, 1);
        addItem(writer, "b", nullptr, 2);
        addItem(writer, "c", &seven, 3);
        addItem(writer, "d", nullptr, 4);
        writer.submit();

        jobSystem.terminate();
    }

    void runQuery(std::string_view query, RowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Item a's own value is left out: an unsigned integer past the largest signed one meets a
// signed fallback in one column, which holds no such value
TEST_F(CoalesceUnsignedArgumentTest, fillsEachNullRowWithItsOwnFallback) {
    RowSink sink;
    runQuery("MATCH (n:Item) RETURN n.name, coalesce(n.unsigned, n.fallback)", sink);

    Rows rows;
    sink.sortedRows(rows);
    ASSERT_EQ(rows.size(), 4u);

    const Rows expected = {{"b", "2"}, {"c", "7"}, {"d", "4"}};
    const Rows besideA(rows.begin() + 1, rows.end());

    EXPECT_EQ(rows.front().front(), "a");
    EXPECT_EQ(besideA, expected);
}
