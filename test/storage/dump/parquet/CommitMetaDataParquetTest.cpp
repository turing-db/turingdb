#include "TuringTest.h"

#include "dump/parquet/CommitMetaDataParquetDumper.h"
#include "dump/parquet/CommitMetaDataParquetLoader.h"
#include "versioning/VersionController.h"
#include "versioning/Commit.h"
#include "versioning/CommitData.h"
#include "versioning/DataPartID.h"
#include "datapart/DataPart.h"
#include "datapart/DataPartSpan.h"
#include "writers/GraphWriter.h"
#include "Graph.h"

#include <stddef.h>

#include <memory>
#include <string>

#include "JobSystem.h"
#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

// Builds a two-commit graph and round-trips the metadata of every data-bearing commit.
// Commit metadata is read off a real Commit (built via GraphWriter), as in
// DataPartParquetPatchTest.
class CommitMetaDataParquetTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "graph";
        _graph = Graph::create("commitmeta", _graphPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const NodeID a = writer.addNode({"Person"});
        const NodeID b = writer.addNode({"Person"});
        writer.addEdge("KNOWS", a, b);
        writer.submit();

        const NodeID c = writer.addNode({"Person"});
        writer.addEdge("KNOWS", c, a);
        writer.submit();
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
};

TEST_F(CommitMetaDataParquetTest, RoundTrip) {
    const VersionController& controller = _graph->getVersionController();
    const Commit* commit = controller.getCommitSafe(controller.getHeadHash());
    ASSERT_NE(commit, nullptr);

    // Dump/load reuse the same file per iteration; each commit is verified before the
    // next overwrites it.
    const fs::Path commitDir = fs::Path(_outDir);

    bool sawDataParts = false;
    while (commit != nullptr) {
        if (commit->hasData()) {
            CommitMetaDataParquetDumper::dump(*commit, commitDir);

            CommitParquetMetaData loaded;
            CommitMetaDataParquetLoader::load(commitDir, loaded);

            EXPECT_EQ(loaded.getNumNodes(), commit->getNumNodes());
            EXPECT_EQ(loaded.getNumEdges(), commit->getNumEdges());
            EXPECT_EQ(loaded.getNumCommitDataParts(), commit->data().commitDataparts().size());

            const DataPartSpan allDataParts = commit->data().allDataparts();
            ASSERT_EQ(loaded.getAllDatapartIds().size(), allDataParts.size());
            for (size_t i = 0; i < allDataParts.size(); ++i) {
                EXPECT_EQ(loaded.getAllDatapartIds()[i].get(), allDataParts[i]->getID().get());
            }

            if (!allDataParts.empty()) {
                sawDataParts = true;
            }
        }

        commit = commit->getPreviousCommit();
    }

    EXPECT_TRUE(sawDataParts);
}
