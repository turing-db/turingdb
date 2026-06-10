#include "EdgeContainerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "EdgeContainerParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = edgeContainerParquetLayout;

namespace {

// One edge file's contents: the EdgeRecord vector plus the first edge/node ids from
// the file metadata, with flags marking whether those ids were present.
struct EdgeRecordsData {
    std::vector<EdgeRecord> _edges;
    uint64_t _firstEdgeID {0};
    uint64_t _firstNodeID {0};
    bool _hasFirstEdgeID {false};
    bool _hasFirstNodeID {false};
};

// One edge file: four INT64 columns (edge_id, node_id, other_id, edge_type_id), plus the
// first edge/node ids in metadata. The reader delivers the columns in schema order, each in
// row order across all row groups, so each column's batches are written straight into the
// caller's EdgeRecord vector through its own cursor. Accumulating the four columns separately
// first would cost an extra 32 bytes per edge (tens of gigabytes on large graphs), all
// transient on top of the records being built.
class EdgeRecordsVisitor : public ParquetSaxVisitor {
public:
    explicit EdgeRecordsVisitor(EdgeRecordsData& data)
        : _data(data) {
    }

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                const std::string& key = keyValueMetadata->key(i);
                if (key == layout::FIRST_EDGE_ID_KEY) {
                    _data._firstEdgeID = parseMetadataUint64(key, keyValueMetadata->value(i));
                    _data._hasFirstEdgeID = true;
                } else if (key == layout::FIRST_NODE_ID_KEY) {
                    _data._firstNodeID = parseMetadataUint64(key, keyValueMetadata->value(i));
                    _data._hasFirstNodeID = true;
                }
            }
        }

        const int64_t numRows = metadata.num_rows();
        if (numRows < 0) {
            throw FatalException("EdgeContainerParquetLoader: negative row count");
        }
        _data._edges.resize(static_cast<size_t>(numRows));

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex > 3) {
            throw FatalException("EdgeContainerParquetLoader: unexpected edge column");
        }

        size_t& cursor = _cursors[columnIndex];
        if (cursor + values.size() > _data._edges.size()) {
            throw FatalException("EdgeContainerParquetLoader: edge column longer than row count");
        }

        if (columnIndex == 0) {
            for (const int64_t value : values) {
                _data._edges[cursor++]._edgeID = EdgeID {static_cast<uint64_t>(value)};
            }
        } else if (columnIndex == 1) {
            for (const int64_t value : values) {
                _data._edges[cursor++]._nodeID = NodeID {static_cast<uint64_t>(value)};
            }
        } else if (columnIndex == 2) {
            for (const int64_t value : values) {
                _data._edges[cursor++]._otherID = NodeID {static_cast<uint64_t>(value)};
            }
        } else {
            for (const int64_t value : values) {
                _data._edges[cursor++]._edgeTypeID = EdgeTypeID {static_cast<uint64_t>(value)};
            }
        }

        return true;
    }

    // Each column must have filled exactly every record; a short column means a truncated
    // file delivered fewer values than the row count promised.
    void checkComplete() const {
        for (size_t columnIndex = 0; columnIndex < 4; ++columnIndex) {
            if (_cursors[columnIndex] != _data._edges.size()) {
                throw FatalException(fmt::format(
                    "EdgeContainerParquetLoader: column {} delivered {} of {} edges",
                    columnIndex, _cursors[columnIndex], _data._edges.size()));
            }
        }
    }

private:
    EdgeRecordsData& _data;
    size_t _cursors[4] {0, 0, 0, 0};
};

void readEdgeFile(const fs::Path& path, EdgeRecordsData& data) {
    EdgeRecordsVisitor visitor(data);

    ParquetWriteSchema expectedSchema;
    expectedSchema.addColumn(layout::EDGE_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::NODE_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::OTHER_ID_COLUMN, ParquetColumnType::UInt64);
    expectedSchema.addColumn(layout::EDGE_TYPE_ID_COLUMN, ParquetColumnType::UInt64);

    ParquetReader reader(path, visitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }

    visitor.checkComplete();
}

}

std::unique_ptr<EdgeContainer> EdgeContainerParquetLoader::load(const fs::Path& outPath,
                                                               const fs::Path& inPath) {
    EdgeRecordsData out;
    readEdgeFile(outPath, out);

    EdgeRecordsData in;
    readEdgeFile(inPath, in);

    const bool outHasFirstIds = out._hasFirstEdgeID && out._hasFirstNodeID;
    const bool inHasFirstIds = in._hasFirstEdgeID && in._hasFirstNodeID;
    if (!outHasFirstIds || !inHasFirstIds) {
        throw FatalException("EdgeContainerParquetLoader: missing first-id metadata");
    }

    // The dumper writes the same first ids to both files; a disagreement means one of
    // them belongs to a different container.
    const bool firstIdsAgree = in._firstEdgeID == out._firstEdgeID
                               && in._firstNodeID == out._firstNodeID;
    if (!firstIdsAgree) {
        throw FatalException("EdgeContainerParquetLoader: out- and in-edge first ids disagree");
    }

    EdgeContainer* container = new EdgeContainer {
        out._firstNodeID,
        out._firstEdgeID,
        std::move(out._edges),
        std::move(in._edges),
    };

    return std::unique_ptr<EdgeContainer>(container);
}
