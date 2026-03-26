#include "TuringTest.h"

#include "dump/GraphDumper.h"
#include "dump/DumpResult.h"
#include "Path.h"
#include "TuringException.h"

#include "dump/GraphLoader.h"
#include "SimpleGraph.h"
#include "Graph.h"
#include "comparators/GraphComparator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

class GraphLoaderTest : public TuringTest {
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

    void dumpGraph() {
        GraphDumper dumper;

        auto res = dumper.dump(*_graph, _dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }
};

TEST_F(GraphLoaderTest, SimpleDumpLoad) {
    dumpGraph();

    auto loadedGraph = Graph::create();
    const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
    if (!loadRes) {
        throw TuringException("Failed to load graph:\n" + loadRes.error().fmtMessage());
    }

    ASSERT_TRUE(GraphComparator::same(*_graph, *loadedGraph));
}

TEST_F(GraphLoaderTest, DumpLoadPreservesDatapartIDs) {
    dumpGraph();

    auto loadedGraph = Graph::create();
    const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
    ASSERT_TRUE(loadRes) << loadRes.error().fmtMessage();

    const auto originalTx = _graph->openTransaction();
    const auto loadedTx = loadedGraph->openTransaction();

    const auto originalParts = originalTx.readGraph().dataparts();
    const auto loadedParts = loadedTx.readGraph().dataparts();

    ASSERT_EQ(originalParts.size(), loadedParts.size());
    for (size_t i = 0; i < originalParts.size(); ++i) {
        ASSERT_EQ(originalParts[i]->getID().get(), loadedParts[i]->getID().get())
            << "Mismatching datapart ID at index " << i;
    }

}

TEST_F(GraphLoaderTest, MissingHeadDatapartPropagatesLoadError) {
    dumpGraph();

    const fs::Path datapartsDir = _dumpPath / "dataparts";
    const auto dataparts = datapartsDir.listDir();
    ASSERT_TRUE(dataparts) << "Failed to list dumped dataparts";
    ASSERT_FALSE(dataparts->empty()) << "Expected dumped graph to contain dataparts";

    const auto rmRes = dataparts->front().rm();
    ASSERT_TRUE(rmRes) << "Failed to remove dumped datapart";

    auto loadedGraph = Graph::create();
    const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
    ASSERT_FALSE(loadRes);
    ASSERT_EQ(loadRes.error().getType(), DumpErrorType::DATAPART_DOES_NOT_EXIST);
}

TEST_F(GraphLoaderTest, MissingHeadJournalPropagatesLoadError) {
    dumpGraph();

    const CommitHash headHash = _graph->getHeadHash();
    const fs::Path journalFile = _dumpPath / "commits" / std::to_string(headHash.get()) / "journal";

    const auto rmRes = journalFile.rm();
    ASSERT_TRUE(rmRes) << "Failed to remove dumped journal";

    auto loadedGraph = Graph::create();
    const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
    ASSERT_FALSE(loadRes);
    ASSERT_EQ(loadRes.error().getType(), DumpErrorType::CANNOT_OPEN_JOURNAL);
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 3; });
}
