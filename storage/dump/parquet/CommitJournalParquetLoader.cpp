#include "CommitJournalParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <vector>

#include "ParquetReader.h"

#include "CommitParquetLayout.h"
#include "versioning/CommitJournal.h"
#include "versioning/WriteSet.h"
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
void loadWriteSet(const fs::Path& path, std::vector<IDT>& out) {
    IdColumnVisitor visitor;
    {
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

    out.reserve(visitor._ids.size());
    for (const int64_t value : visitor._ids) {
        out.push_back(IDT {static_cast<typename IDT::Type>(value)});
    }
}

}

void CommitJournalParquetLoader::load(const fs::Path& commitDir, CommitJournal& out) {
    loadWriteSet<NodeID>(commitParquetLayout::journalNodes(commitDir), out.rawNodeWriteSet());
    loadWriteSet<EdgeID>(commitParquetLayout::journalEdges(commitDir), out.rawEdgeWriteSet());
}
