#include "GraphComparator.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "comparators/CommitViewComparator.h"
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
        const auto& commitsA = readerA.commits();
        const auto& commitsB = readerB.commits();

        const size_t szA = commitsA.size();
        const size_t szB = commitsB.size();

        if (szA != szB) {
            spdlog::error("Graph A has {} commits whilst Graph B has {} commits.", szA,
                          szB);
            return false;
        }

        auto itA = commitsA.begin();
        auto itB = commitsB.begin();
        size_t index = 0;
        while (itA != commitsA.end() && itB != commitsB.end()) {
            if (!CommitViewComparator::same(*itA, *itB)) {
                spdlog::error("Graph A commit at index {} differs from Graph B commit.",
                              index);
                return false;
            }

            itA++;
            itB++;
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

