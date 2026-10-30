#include "EdgeCountsLoader.h"

#include <optional>
#include <vector>

#include "datapart/EdgeCounts.h"
#include "metadata/GraphMetadata.h"
#include "EdgeCountsDumpConstants.h"
#include "GraphDumpHelper.h"
#include "LoadUtils.h"

using namespace db;

EdgeCountsLoader::EdgeCountsLoader(fs::FilePageReader& reader)
    : _reader(reader)
{
}

EdgeCountsLoader::~EdgeCountsLoader() {
}

DumpResult<void> EdgeCountsLoader::load(const GraphMetadata& metadata, EdgeCounts& counts) {
    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_COUNTS,
                                 _reader.error().value());
    }

    auto it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    LoadUtils::ensureLoadSpace(EdgeCountsDumpConstants::ENTRY_COUNT_STRIDE, _reader, it);
    const uint64_t entryCount = it.get<uint64_t>();

    const LabelSetMap& labelSets = metadata.labelsets();

    std::vector<EdgeCount> entries;
    entries.reserve(entryCount);

    for (uint64_t i = 0; i < entryCount; i++) {
        LoadUtils::ensureLoadSpace(EdgeCountsDumpConstants::ENTRY_STRIDE, _reader, it);

        const EdgeTypeID edgeType = it.get<EdgeTypeID::Type>();
        const LabelSetID sourceID = it.get<LabelSetID::Type>();
        const LabelSetID targetID = it.get<LabelSetID::Type>();
        const uint64_t edgeCount = it.get<uint64_t>();

        const std::optional<LabelSetHandle> source = labelSets.getValue(sourceID);
        const std::optional<LabelSetHandle> target = labelSets.getValue(targetID);
        if (!source || !target) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_COUNTS);
        }

        entries.push_back(EdgeCount {._edgeTypeID = edgeType,
                                     ._sourceLabelSet = *source,
                                     ._targetLabelSet = *target,
                                     ._count = edgeCount});
    }

    counts.assign(entries);

    return {};
}
