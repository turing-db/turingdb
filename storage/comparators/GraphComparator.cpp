#include "GraphComparator.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "comparators/CommitViewComparator.h"
#include "versioning/VersionController.h"
#include "DataPartComparator.h"
#include "GraphMetadataComparator.h"

using namespace db;

bool GraphComparator::same(const Graph& a, const Graph& b) {
    if (a.getName() != b.getName()) {
        return false;
    }

    const FrozenCommitTx txA = a.openTransaction();
    const FrozenCommitTx txB = b.openTransaction();
    const GraphReader readerA = txA.readGraph();
    const GraphReader readerB = txB.readGraph();

    if (!GraphMetadataComparator::same(readerA.getMetadata(), readerB.getMetadata())) {
        return false;
    }

    { // Verifiying commits are the same
        const auto& controllerA = a.getVersionController();
        const auto& controllerB = b.getVersionController();

        const size_t szA = controllerA.getNumCommits();
        const size_t szB = controllerB.getNumCommits();

        if (szA != szB) {
            spdlog::error("Graph A has {} commits whilst Graph B has {} commits.", szA,
                          szB);
            return false;
        }

        const Commit* commitA = controllerA.getCommitSafe(controllerA.getHeadHash());
        bioassert(commitA, "headHash of Graph A not found");

        const Commit* commitB = controllerB.getCommitSafe(controllerB.getHeadHash());
        bioassert(commitB, "headHash of Graph B not found");

        size_t index = 0;
        while (commitA != nullptr && commitB != nullptr) {
            if (commitA->hasData() && commitB->hasData()) {
                if (!CommitComparator::same(commitA, commitB)) {
                    spdlog::error("Graph A commit at index {} differs from Graph B commit.",
                                  index);
                    return false;
                }
            }
            
            if(commitA->getNumEdges() != commitB->getNumEdges()) {
                spdlog::error("At Index {} commitA and commitB have different number of edges",
                              index);
                return false;
            }

            if(commitA->getNumNodes() != commitB->getNumNodes()) {
                spdlog::error("At Index {} commitA and commitB have different number of nodes",
                              index);
                return false;
            }

            if(commitA->getNumDataParts() != commitB->getNumDataParts()) {
                spdlog::error("At Index {} commitA and commitB have different number of dataparts",
                              index);
                return false;
            }

            commitA = commitA->getPreviousCommit();
            commitB = commitB->getPreviousCommit();
            index++;
        }

        return true;
    }

    const DataPartSpan partsA = readerA.dataparts();
    const DataPartSpan partsB = readerB.dataparts();

    if (partsA.size() != partsB.size()) {
        return false;
    }

    for (size_t i = 0; i < partsA.size(); i++) {
        if (!DataPartComparator::same(*partsA[i], *partsB[i])) {
            return false;
        }
    }

    return true;
}

