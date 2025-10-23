#include "TombstonesDumper.h"

#include "GraphDumpHelper.h"

#include "versioning/Tombstones.h"
#include "dump/DumpUtils.h"

using namespace db;

DumpResult<void> TombstonesDumper::dump(const Tombstones& tombstones) {
    GraphDumpHelper::writeFileHeader(_writer);

    { // Dump node tombstones
        const Tombstones::NodeTombstones& nodeTombstones = tombstones.nodeTombstones();
        const size_t nodesSize = nodeTombstones.size();

        DumpUtils::ensureDumpSpace(sizeof(nodesSize), _writer);
        _writer.writeToCurrentPage(nodesSize);

        DumpUtils::dumpRange(nodeTombstones, _writer);
    }

    { // Dump edge tombstones
        const Tombstones::EdgeTombstones& edgeTombstones = tombstones.edgeTombstones();
        const size_t edgesSize = edgeTombstones.size();

        DumpUtils::ensureDumpSpace(sizeof(edgesSize), _writer);
        _writer.writeToCurrentPage(edgesSize);

        DumpUtils::dumpRange(edgeTombstones, _writer);
    }

    return {};
}
