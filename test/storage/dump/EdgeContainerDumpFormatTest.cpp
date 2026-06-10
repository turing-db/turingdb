#include "TuringTest.h"

#include <algorithm>
#include <unordered_map>

#include "datapart/EdgeContainer.h"
#include "dump/DumpConfig.h"
#include "dump/EdgeContainerDumper.h"
#include "dump/EdgeContainerLoader.h"
#include "FileHash.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"

using namespace db;
using namespace turing::test;

// Pins the on-disk edge container dump byte format with a content hash, like
// PropertyDumpFormatTest does for property containers. The expected hash was
// produced by the per-field dumper that bulk writes replaced, so a failure
// means the byte format changed: either an accidental byte-stream regression
// in the dumper, or a deliberate format change — in which case bump
// DumpConfig::VERSION and regenerate the hash.
class EdgeContainerDumpFormatTest : public TuringTest {
};

TEST_F(EdgeContainerDumpFormatTest, edgeContainerDumpBytes) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "edges";

    // 70'000 edges span three pages per direction.
    std::vector<EdgeRecord> outEdges;
    outEdges.reserve(70'000);
    for (size_t i = 0; i < 70'000; i++) {
        outEdges.push_back({EdgeID(i), NodeID(i % 9'973), NodeID((i * 7 + 13) % 9'973), EdgeTypeID(i % 5)});
    }

    std::unordered_map<EdgeID, EdgeID> tmpToFinalEdgeIDs;
    const auto edges = EdgeContainer::create(NodeID(0), EdgeID(0), std::move(outEdges), tmpToFinalEdgeIDs);
    ASSERT_TRUE(edges);

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EdgeContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(*edges));
    }

    uint64_t hash = 0;
    hashDumpFileContent(path, hash);
    EXPECT_EQ(hash, 14089840393657971070ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        EdgeContainerLoader loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto sameRecord = [](const EdgeRecord& first, const EdgeRecord& second) {
            return first._edgeID == second._edgeID
                && first._nodeID == second._nodeID
                && first._otherID == second._otherID
                && first._edgeTypeID == second._edgeTypeID;
        };

        const auto& loaded = *result.value();
        ASSERT_EQ(loaded.size(), edges->size());
        ASSERT_TRUE(std::ranges::equal(loaded.getOuts(), edges->getOuts(), sameRecord));
        ASSERT_TRUE(std::ranges::equal(loaded.getIns(), edges->getIns(), sameRecord));
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
