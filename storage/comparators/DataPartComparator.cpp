#include "DataPartComparator.h"

#include "DataPart.h"
#include "NodeContainerComparator.h"
#include "EdgeContainerComparator.h"
#include "EdgeIndexerComparator.h"
#include "PropertyManagerComparator.h"
#include "StringApproxIndexerComparator.h"
#include "comparators/StringApproxIndexerComparator.h"

using namespace db;

bool DataPartComparator::same(const DataPart& a, const DataPart& b) {
    if (a.getFirstNodeID() != b.getFirstNodeID()) {
        return false;
    }

    if (a.getFirstEdgeID() != b.getFirstEdgeID()) {
        return false;
    }

    if (!NodeContainerComparator::same(a.nodes(), b.nodes())) {
        return false;
    }

    if (!EdgeContainerComparator::same(a.edges(), b.edges())) {
        return false;
    }

    if (!EdgeIndexerComparator::same(a.edgeIndexer(), b.edgeIndexer())) {
        return false;
    }

    if (!PropertyManagerComparator::same(a.nodeProperties(), b.nodeProperties())) {
        return false;
    }

    if (!PropertyManagerComparator::same(a.edgeProperties(), b.edgeProperties())) {
        return false;
    }

    if (!StringApproxIndexerComparator::same(a.getNodeStrPropIndexer(),
                                             b.getNodeStrPropIndexer())) {
        return false;
    }

    if (!StringApproxIndexerComparator::same(a.getEdgeStrPropIndexer(),
                                             b.getEdgeStrPropIndexer())) {
        return false;
    }

    return true;
}
