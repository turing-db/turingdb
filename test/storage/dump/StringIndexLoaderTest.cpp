#include "TuringTest.h"

#include "Graph.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "SimpleGraph.h"
#include "comparators/GraphComparator.h"
#include "indexers/StringPropertyIndexer.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "Path.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

class StringIndexLoaderTest : public TuringTest {
public:
    void initialize() override {
        _originalPath = fs::Path {_outDir} / "simple";
        _dumpPath = fs::Path {_outDir} / "dump";

        _graph = Graph::create("simple", _originalPath);
        SimpleGraph::createSimpleGraph(_graph.get());
    }

protected:
    fs::Path _originalPath;
    fs::Path _dumpPath;

    std::unique_ptr<Graph> _graph;
};

TEST_F(StringIndexLoaderTest, SimpleDumpLoad) {
    GraphDumper dumper;

    auto res = dumper.dump(*_graph, _dumpPath);
    if (!res) {
        throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
    }

    auto loadedGraph = Graph::create();
    const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
    if (!loadRes) {
        throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
    }

    ASSERT_TRUE(GraphComparator::same(*_graph, *loadedGraph));

    auto tx = _graph->openTransaction();
    auto reader = tx.readGraph();
    auto builtDps = reader.dataparts();

    for (const auto& dp : builtDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }

    auto txl = loadedGraph->openTransaction();
    auto readerl = txl.readGraph();
    auto loadedDps = readerl.dataparts();

    for (const auto& dp : loadedDps) {
        EXPECT_TRUE(dp->getEdgeStrPropIndexer().isInitialised());
        EXPECT_TRUE(dp->getNodeStrPropIndexer().isInitialised());
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 3; });
}
