#include "DataPartLimit.h"

#include <spdlog/fmt/fmt.h>

#include "Graph.h"
#include "TuringConfig.h"
#include "PipelineException.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"

#include "BioAssert.h"

using namespace db;

void db::throwIfTooManyDataParts(const ChangeAccessor& access, const TuringConfig* config) {
    bioassert(config, "throwIfTooManyDataParts: config must be set");

    const Graph* graph = access.getGraph();
    bioassert(graph, "throwIfTooManyDataParts: graph must be set");

    const size_t maxDataPartsCount = config->getMaxDataParts();

    // Data parts currently living on the graph's main head.
    const FrozenCommitTx headTx = graph->openTransaction();
    const CommitData* headData = headTx.commitData().get();
    const size_t mainDataPartsCount = headData->allDataparts().size();

    // Data parts the in-progress change has already committed but not yet submitted.
    size_t changeDataPartsCount = 0;
    for (const auto& commitBuilder : access.pendingCommits()) {
        changeDataPartsCount += commitBuilder->dpCount();
    }

    const size_t totalDataPartsCount = mainDataPartsCount + changeDataPartsCount;

    if (totalDataPartsCount >= maxDataPartsCount) {
        throw PipelineException(fmt::format(
            "Graph '{}' has {} data parts, reaching the limit of {}. "
            "Run MERGE_DATAPARTS to compact the graph before writing more data.",
            graph->getName(),
            totalDataPartsCount,
            maxDataPartsCount));
    }
}
