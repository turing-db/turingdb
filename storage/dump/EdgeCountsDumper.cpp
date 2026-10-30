#include "EdgeCountsDumper.h"

#include "datapart/EdgeCounts.h"
#include "EdgeCountsDumpConstants.h"
#include "GraphDumpHelper.h"
#include "dump/DumpUtils.h"

using namespace db;

EdgeCountsDumper::EdgeCountsDumper(fs::FilePageWriter& writer)
    : _writer(writer)
{
}

EdgeCountsDumper::~EdgeCountsDumper() {
}

DumpResult<void> EdgeCountsDumper::dump(const EdgeCounts& counts) {
    GraphDumpHelper::writeFileHeader(_writer);

    const std::span<const EdgeCount> entries = counts.getCounts();
    const uint64_t entryCount = entries.size();

    DumpUtils::ensureDumpSpace(EdgeCountsDumpConstants::ENTRY_COUNT_STRIDE, _writer);
    _writer.writeToCurrentPage(entryCount);

    for (const EdgeCount& count : entries) {
        const uint64_t edgeCount = count._count;

        DumpUtils::ensureDumpSpace(EdgeCountsDumpConstants::ENTRY_STRIDE, _writer);
        _writer.writeToCurrentPage(count._edgeTypeID.getValue());
        _writer.writeToCurrentPage(count._sourceLabelSet.getID().getValue());
        _writer.writeToCurrentPage(count._targetLabelSet.getID().getValue());
        _writer.writeToCurrentPage(edgeCount);
    }

    return {};
}
