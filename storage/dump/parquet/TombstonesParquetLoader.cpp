#include "TombstonesParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "CommitParquetLayout.h"
#include "versioning/TombstoneSet.h"
#include "versioning/Tombstones.h"
#include "ID.h"
#include "Path.h"

using namespace db;

namespace {

// A single INT64 id column (node_id or edge_id), collected across all chunks.
class IdColumnVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _ids;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _ids.push_back(value);
        }
        return true;
    }
};

template <typename IDT>
void loadTombstoneSet(const fs::Path& path, std::string_view columnName, TombstoneSet<IDT>& out) {
    IdColumnVisitor visitor;
    {
        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(columnName, ParquetColumnType::UInt64);

        ParquetReader reader(path, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    std::vector<IDT> ids;
    ids.reserve(visitor._ids.size());
    for (const int64_t value : visitor._ids) {
        ids.push_back(IDT {static_cast<typename IDT::Type>(value)});
    }

    out.reserve(ids.size());
    out.insert(ids);
}

}

void TombstonesParquetLoader::load(const fs::Path& commitDir, Tombstones& out) {
    loadTombstoneSet<NodeID>(commitParquetLayout::tombstoneNodes(commitDir),
                             commitParquetLayout::NODE_ID_COLUMN,
                             out.nodeTombstones());
    loadTombstoneSet<EdgeID>(commitParquetLayout::tombstoneEdges(commitDir),
                             commitParquetLayout::EDGE_ID_COLUMN,
                             out.edgeTombstones());
}
