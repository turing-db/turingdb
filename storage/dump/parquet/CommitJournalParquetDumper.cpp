#include "CommitJournalParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "CommitParquetLayout.h"
#include "versioning/CommitJournal.h"
#include "versioning/WriteSet.h"
#include "ID.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view EDGE_ID_COLUMN = "edge_id";

template <typename WriteSetType>
void dumpWriteSet(const WriteSetType& writeSet, std::string_view columnName, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(columnName, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    const size_t count = writeSet.size();
    if (count > 0) {
        std::vector<int64_t> ids;
        ids.reserve(count);

        for (const auto id : writeSet) {
            ids.push_back(static_cast<int64_t>(id.getValue()));
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
    }

    writer.finish();
}

}

void CommitJournalParquetDumper::dump(const CommitJournal& journal, const fs::Path& commitDir) {
    dumpWriteSet(journal.nodeWriteSet(), NODE_ID_COLUMN, commitParquetLayout::journalNodes(commitDir));
    dumpWriteSet(journal.edgeWriteSet(), EDGE_ID_COLUMN, commitParquetLayout::journalEdges(commitDir));
}
