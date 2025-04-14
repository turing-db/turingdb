#include "GraphComparator.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "DataPartComparator.h"
#include "GraphMetadataComparator.h"
#include "versioning/Transaction.h"

using namespace db;


bool GraphComparator::same(const Graph& a, const Graph& b) {
    if (a.getName() != b.getName()) {
        return false;
    }

    const Graph::EntityIDs idsA = a.getNextFreeIDs();
    const Graph::EntityIDs idsB = b.getNextFreeIDs();

    const Transaction txA = a.openTransaction();
    const Transaction txB = b.openTransaction();
    const GraphReader readerA = txA.readGraph();
    const GraphReader readerB = txB.readGraph();

    if (!GraphMetadataComparator::same(readerA.getMetadata(), readerB.getMetadata())) {
        return false;
    }

    if (idsA._node != idsB._node) {
        return false;
    }

    if (idsA._edge != idsB._edge) {
        return false;
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

