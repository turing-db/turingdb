#include "CommitLoader.h"

#include "BinaryDiskDecoder.h"
#include "GraphLoader.h"
#include "versioning/Commit.h"

namespace db {

DumpResult<void> CommitLoader::loadData(const fs::Path& commitDir,
                                        const fs::Path& partsDir,
                                        Graph* graph,
                                        Commit* commit) {
    GraphLoader builder(graph);
    BinaryDiskDecoder decoder(&builder);
    return decoder.decodeCommitData(commitDir, partsDir, commit->hash().get());
}

}
