#include "CommitJournalParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "CommitParquetLayout.h"
#include "versioning/CommitJournal.h"
#include "versioning/WriteSet.h"
#include "ID.h"
#include "Path.h"

using namespace db;

namespace {

// A single INT64 id column (node_id or edge_id), collected across all chunks into
// the caller-owned vector.
class IdColumnVisitor : public ParquetSaxVisitor {
public:
    explicit IdColumnVisitor(std::vector<int64_t>& ids)
        : _ids(ids) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _ids.push_back(value);
        }
        return true;
    }

private:
    std::vector<int64_t>& _ids;
};

template <typename IDT>
void loadWriteSet(const fs::Path& path, std::string_view columnName, std::vector<IDT>& out) {
    std::vector<int64_t> ids;
    {
        IdColumnVisitor visitor(ids);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(columnName, ParquetColumnType::UInt64);

        ParquetReader reader(path, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    out.reserve(ids.size());
    for (const int64_t value : ids) {
        out.push_back(IDT {static_cast<typename IDT::Type>(value)});
    }
}

}

void CommitJournalParquetLoader::load(const fs::Path& commitDir, CommitJournal& out) {
    loadWriteSet<NodeID>(commitParquetLayout::journalNodes(commitDir),
                         commitParquetLayout::NODE_ID_COLUMN,
                         out.rawNodeWriteSet());
    loadWriteSet<EdgeID>(commitParquetLayout::journalEdges(commitDir),
                         commitParquetLayout::EDGE_ID_COLUMN,
                         out.rawEdgeWriteSet());
}
