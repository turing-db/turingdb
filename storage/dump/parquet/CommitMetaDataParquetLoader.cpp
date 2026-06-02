#include "CommitMetaDataParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>

#include "ParquetReader.h"

#include "CommitParquetLayout.h"
#include "versioning/DataPartID.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view NUM_NODES_KEY = "turing.num_nodes";
constexpr std::string_view NUM_EDGES_KEY = "turing.num_edges";
constexpr std::string_view NUM_COMMIT_DATAPARTS_KEY = "turing.num_commit_dataparts";

// Reads the three scalars from the file's key/value metadata and the data_part_id column.
class CommitMetaDataVisitor : public ParquetSaxVisitor {
public:
    size_t _numNodes {0};
    size_t _numEdges {0};
    size_t _numCommitDataParts {0};
    std::vector<int64_t> _datapartIds;

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        if (!keyValueMetadata) {
            throw FatalException("CommitMetaDataParquetLoader: file has no key/value metadata");
        }

        bool hasNumNodes = false;
        bool hasNumEdges = false;
        bool hasNumCommitDataParts = false;
        for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
            const std::string& key = keyValueMetadata->key(i);
            const std::string& value = keyValueMetadata->value(i);
            if (key == NUM_NODES_KEY) {
                _numNodes = static_cast<size_t>(std::stoull(value));
                hasNumNodes = true;
            } else if (key == NUM_EDGES_KEY) {
                _numEdges = static_cast<size_t>(std::stoull(value));
                hasNumEdges = true;
            } else if (key == NUM_COMMIT_DATAPARTS_KEY) {
                _numCommitDataParts = static_cast<size_t>(std::stoull(value));
                hasNumCommitDataParts = true;
            }
        }

        if (!hasNumNodes || !hasNumEdges || !hasNumCommitDataParts) {
            throw FatalException("CommitMetaDataParquetLoader: missing commit-metadata scalar");
        }

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _datapartIds.push_back(value);
        }
        return true;
    }
};

}

void CommitMetaDataParquetLoader::load(const fs::Path& commitDir, CommitParquetMetaData& out) {
    CommitMetaDataVisitor visitor;
    {
        ParquetReader reader(commitParquetLayout::commitMetaData(commitDir), visitor);
        while (reader.nextChunk()) {
        }
    }

    out._numNodes = visitor._numNodes;
    out._numEdges = visitor._numEdges;
    out._numCommitDataParts = visitor._numCommitDataParts;

    out._allDatapartIds.reserve(visitor._datapartIds.size());
    for (const int64_t value : visitor._datapartIds) {
        out._allDatapartIds.push_back(DataPartID {static_cast<DataPartID::ValueType>(value)});
    }
}
