#include "CommitMetaDataParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "CommitParquetLayout.h"
#include "datapart/DataPart.h"
#include "datapart/DataPartSpan.h"
#include "versioning/Commit.h"
#include "versioning/CommitData.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view DATA_PART_ID_COLUMN = "data_part_id";
constexpr std::string_view NUM_NODES_KEY = "turing.num_nodes";
constexpr std::string_view NUM_EDGES_KEY = "turing.num_edges";
constexpr std::string_view NUM_COMMIT_DATAPARTS_KEY = "turing.num_commit_dataparts";

}

void CommitMetaDataParquetDumper::dump(const Commit& commit, const fs::Path& commitDir) {
    const size_t numNodes = commit.getNumNodes();
    const size_t numEdges = commit.getNumEdges();

    // A shell commit (a lazily-loaded ancestor — only the head commit is materialized on
    // load) has no CommitData in memory, just its counters. Its datapart id list is never
    // read back (only the head commit's loadData consumes it), so write an empty column and
    // take the commit-datapart count from the commit's own counter.
    const bool hasData = commit.hasData();
    const size_t numCommitDataParts = hasData ? commit.data().commitDataparts().size()
                                              : commit.getNumDataParts();

    ParquetWriteSchema schema;
    schema.addColumn(DATA_PART_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(commitParquetLayout::commitMetaData(commitDir), schema);
    writer.setMetadata(NUM_NODES_KEY, std::to_string(numNodes));
    writer.setMetadata(NUM_EDGES_KEY, std::to_string(numEdges));
    writer.setMetadata(NUM_COMMIT_DATAPARTS_KEY, std::to_string(numCommitDataParts));

    if (hasData) {
        const DataPartSpan allDataParts = commit.data().allDataparts();
        const size_t count = allDataParts.size();
        if (count > 0) {
            std::vector<int64_t> ids;
            ids.reserve(count);

            for (const auto& part : allDataParts) {
                ids.push_back(static_cast<int64_t>(part->getID().get()));
            }

            writer.beginRowGroup(count);
            writer.writeInt64Column(0, ids);
        }
    }

    writer.finish();
}
