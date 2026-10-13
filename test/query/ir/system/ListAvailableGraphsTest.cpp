#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

struct AvailableGraph {
    std::string name;
    bool isLoaded {false};
    bool isLoading {false};

    auto operator<=>(const AvailableGraph&) const = default;
};

void PrintTo(const AvailableGraph& graph, std::ostream* out) {
    *out << "{" << graph.name << ", isLoaded=" << graph.isLoaded << ", isLoading=" << graph.isLoading << "}";
}

class AvailableGraphSink : public NLOutputSink {
public:
    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        using ViewColumn = ColumnVector<types::String::Primitive>;
        using BoolColumn = ColumnVector<types::Bool::Primitive>;

        const ViewColumn* const names = static_cast<const ViewColumn*>(chunks[0]);
        const BoolColumn* const loaded = static_cast<const BoolColumn*>(chunks[1]);
        const BoolColumn* const loading = static_cast<const BoolColumn*>(chunks[2]);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _graphs.push_back({std::string((*names)[row]),
                               static_cast<bool>((*loaded)[row]),
                               static_cast<bool>((*loading)[row])});
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<AvailableGraph>& getGraphs() const { return _graphs; }

private:
    std::vector<std::string> _columnNames;
    std::vector<AvailableGraph> _graphs;
};

}

class ListAvailableGraphsTest : public TuringTest {
public:
    void initialize() override {
        _turingDir = fs::Path {_outDir} / "turing";
    }

protected:
    void startDatabase(bool syncedOnDisk) {
        _interpreter.reset();
        _env.reset();

        _env = syncedOnDisk ? TuringTestEnv::createSyncedOnDisk(_turingDir)
                            : TuringTestEnv::create(_turingDir);
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

    void createSimpleGraph() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, "default", CommitHash::head(), ChangeID::head(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void listAvailableGraphs(std::vector<AvailableGraph>& graphs) {
        AvailableGraphSink sink;
        runQuery("LIST AVAILABLE GRAPHS", sink);

        const std::vector<std::string> expectedColumnNames = {"graphName", "isLoaded", "isLoading"};
        EXPECT_EQ(sink.getColumnNames(), expectedColumnNames);

        graphs = sink.getGraphs();
        std::ranges::sort(graphs);
    }

    const std::string _graphName = "simpledb";
    fs::Path _turingDir;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListAvailableGraphsTest, inMemoryGraphsAreListedAsLoaded) {
    startDatabase(false);
    createSimpleGraph();

    StringRowSink createA;
    runQuery("CREATE GRAPH a", createA);
    StringRowSink createB;
    runQuery("CREATE GRAPH b", createB);

    std::vector<AvailableGraph> graphs;
    listAvailableGraphs(graphs);

    const std::vector<AvailableGraph> expected = {
        {"a", true, false},
        {"b", true, false},
        {"default", true, false},
        {"simpledb", true, false},
    };
    EXPECT_EQ(graphs, expected);
}

TEST_F(ListAvailableGraphsTest, onDiskGraphsAreListedOnceWhetherLoadedOrNot) {
    startDatabase(true);
    createSimpleGraph();
    startDatabase(true);

    std::vector<AvailableGraph> beforeLoad;
    listAvailableGraphs(beforeLoad);

    const std::vector<AvailableGraph> expectedBeforeLoad = {
        {"default", true, false},
        {"simpledb", false, false},
    };
    EXPECT_EQ(beforeLoad, expectedBeforeLoad);

    StringRowSink load;
    runQuery("LOAD GRAPH simpledb", load);

    StringRowSink create;
    runQuery("CREATE GRAPH other", create);

    std::vector<AvailableGraph> afterLoad;
    listAvailableGraphs(afterLoad);

    const std::vector<AvailableGraph> expectedAfterLoad = {
        {"default", true, false},
        {"other", true, false},
        {"simpledb", true, false},
    };
    EXPECT_EQ(afterLoad, expectedAfterLoad);
}
