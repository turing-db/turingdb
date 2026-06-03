#include "TombstonesParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "CommitParquetLayout.h"
#include "versioning/TombstoneSet.h"
#include "versioning/Tombstones.h"
#include "ID.h"
#include "Path.h"

using namespace db;

namespace {

template <typename TombstoneSetType>
void dumpTombstoneSet(const TombstoneSetType& set, std::string_view columnName, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(columnName, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    const size_t count = set.size();
    if (count > 0) {
        std::vector<int64_t> ids;
        ids.reserve(count);

        for (const auto id : set) {
            ids.push_back(static_cast<int64_t>(id.getValue()));
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, ids);
    }

    writer.finish();
}

}

void TombstonesParquetDumper::dump(const Tombstones& tombstones, const fs::Path& commitDir) {
    dumpTombstoneSet(tombstones.nodeTombstones(),
                     commitParquetLayout::NODE_ID_COLUMN,
                     commitParquetLayout::tombstoneNodes(commitDir));
    dumpTombstoneSet(tombstones.edgeTombstones(),
                     commitParquetLayout::EDGE_ID_COLUMN,
                     commitParquetLayout::tombstoneEdges(commitDir));
}
