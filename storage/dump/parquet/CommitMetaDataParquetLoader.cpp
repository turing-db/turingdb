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
#include "ParquetWriteSchema.h"

#include "CommitParquetLayout.h"
#include "ParquetMetadataParsing.h"
#include "versioning/DataPartID.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = commitParquetLayout;

namespace {

// The three scalars from the file's key/value metadata and the data_part_id column.
struct CommitMetaDataValues {
    size_t _numNodes {0};
    size_t _numEdges {0};
    size_t _numCommitDataParts {0};
    std::vector<int64_t> _datapartIds;
};

// Fills the caller-owned CommitMetaDataValues from the file metadata and data_part_id column.
class CommitMetaDataVisitor : public ParquetSaxVisitor {
public:
    explicit CommitMetaDataVisitor(CommitMetaDataValues& values)
        : _values(values) {
    }

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
            if (key == layout::NUM_NODES_KEY) {
                _values._numNodes = static_cast<size_t>(parseMetadataUint64(key, value));
                hasNumNodes = true;
            } else if (key == layout::NUM_EDGES_KEY) {
                _values._numEdges = static_cast<size_t>(parseMetadataUint64(key, value));
                hasNumEdges = true;
            } else if (key == layout::NUM_COMMIT_DATAPARTS_KEY) {
                _values._numCommitDataParts = static_cast<size_t>(parseMetadataUint64(key, value));
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
            _values._datapartIds.push_back(value);
        }
        return true;
    }

private:
    CommitMetaDataValues& _values;
};

}

void CommitMetaDataParquetLoader::load(const fs::Path& commitDir, CommitParquetMetaData& out) {
    CommitMetaDataValues values;
    {
        CommitMetaDataVisitor visitor(values);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::DATA_PART_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(layout::commitMetaData(commitDir), visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    out._numNodes = values._numNodes;
    out._numEdges = values._numEdges;
    out._numCommitDataParts = values._numCommitDataParts;

    out._allDatapartIds.reserve(values._datapartIds.size());
    for (const int64_t value : values._datapartIds) {
        out._allDatapartIds.push_back(DataPartID {static_cast<DataPartID::ValueType>(value)});
    }
}
