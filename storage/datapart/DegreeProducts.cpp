#include "DegreeProducts.h"

#include <algorithm>
#include <tuple>

#include "DataPart.h"
#include "EdgeContainer.h"
#include "EdgeRecord.h"
#include "NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "Profiler.h"

#include "BioAssert.h"

using namespace db;

namespace {

using ProductTriple = std::tuple<LabelSetID::Type, uint64_t, uint64_t>;

struct DegreeCount {
    TypedDegree _degree;
    size_t _before {0};
    size_t _added {0};
};

constexpr uint32_t NO_SLOT = UINT32_MAX;

// Orders typed degrees by type, a type's out-degree before its in-degree
uint64_t keyOf(const TypedDegree& degree) {
    return 2 * degree._edgeTypeID.getValue() + (degree._isOut ? 0 : 1);
}

// Orders typed degrees by side, out-degrees first, then by type
uint64_t sideKeyOf(const TypedDegree& degree) {
    return (degree._isOut ? 0 : UINT64_C(1) << 63) | degree._edgeTypeID.getValue();
}

ProductTriple tripleOf(const DegreeProduct& product) {
    return ProductTriple {product._labelSet.getID().getValue(), keyOf(product._first), keyOf(product._second)};
}

bool selects(const DegreeSelection& selection, const TypedDegree& degree) {
    if (selection._isOut != degree._isOut) {
        return false;
    }

    return selection._isEveryType || std::ranges::contains(selection._edgeTypes, degree._edgeTypeID);
}

DegreeCount& findDegree(std::vector<DegreeCount>& degrees, const TypedDegree& degree) {
    for (DegreeCount& count : degrees) {
        if (keyOf(count._degree) == keyOf(degree)) {
            return count;
        }
    }

    degrees.push_back(DegreeCount {._degree = degree});

    return degrees.back();
}

void countBefore(std::span<const EdgeRecord> edges, bool areOut, std::vector<DegreeCount>& degrees) {
    for (const EdgeRecord& edge : edges) {
        findDegree(degrees, TypedDegree {._edgeTypeID = edge._edgeTypeID, ._isOut = areOut})._before++;
    }
}

void countAdded(std::span<const EdgeRecord> edges, bool areOut, std::vector<DegreeCount>& degrees) {
    for (const EdgeRecord& edge : edges) {
        findDegree(degrees, TypedDegree {._edgeTypeID = edge._edgeTypeID, ._isOut = areOut})._added++;
    }
}

// What the sum of a pair of degrees gains when a node's edges grow from those before to
// those added as well
size_t productGain(const DegreeCount& first, const DegreeCount& second, bool areOne) {
    const size_t firstAfter = first._before + first._added;
    const size_t secondAfter = second._before + second._added;

    if (areOne) {
        const size_t before = first._before == 0 ? 0 : first._before * (first._before - 1);
        return firstAfter * (firstAfter - 1) - before;
    }

    return firstAfter * secondAfter - first._before * second._before;
}

// The products of the nodes of one label set, held over the typed degrees they have so far
class ProductTally {
public:
    void add(std::span<const DegreeCount> degrees) {
        _slots.clear();
        for (const DegreeCount& degree : degrees) {
            _slots.push_back(slotOf(degree._degree));
        }

        for (size_t i = 0; i < degrees.size(); i++) {
            for (size_t j = i; j < degrees.size(); j++) {
                const bool addsEdges = degrees[i]._added != 0 || degrees[j]._added != 0;
                if (!addsEdges) {
                    continue;
                }

                const size_t gain = productGain(degrees[i], degrees[j], i == j);
                const uint32_t low = std::min(_slots[i], _slots[j]);
                const uint32_t high = std::max(_slots[i], _slots[j]);
                _sums[low * _capacity + high] += gain;
            }
        }
    }

    void flush(const LabelSetHandle& labelSet, std::vector<DegreeProduct>& products) {
        for (size_t low = 0; low < _degrees.size(); low++) {
            for (size_t high = low; high < _degrees.size(); high++) {
                size_t& sum = _sums[low * _capacity + high];
                if (sum == 0) {
                    continue;
                }

                const bool lowFirst = keyOf(_degrees[low]) <= keyOf(_degrees[high]);
                products.push_back(DegreeProduct {._labelSet = labelSet,
                                                  ._first = lowFirst ? _degrees[low] : _degrees[high],
                                                  ._second = lowFirst ? _degrees[high] : _degrees[low],
                                                  ._sum = sum});
                sum = 0;
            }
        }

        for (const TypedDegree& degree : _degrees) {
            _slotOfKey[keyOf(degree)] = NO_SLOT;
        }

        _degrees.clear();
    }

private:
    std::vector<uint32_t> _slotOfKey;
    std::vector<TypedDegree> _degrees;
    std::vector<size_t> _sums;
    size_t _capacity {0};
    std::vector<uint32_t> _slots;

    uint32_t slotOf(const TypedDegree& degree) {
        const uint64_t key = keyOf(degree);
        if (key >= _slotOfKey.size()) {
            _slotOfKey.resize(key + 1, NO_SLOT);
        }

        if (_slotOfKey[key] != NO_SLOT) {
            return _slotOfKey[key];
        }

        if (_degrees.size() == _capacity) {
            grow();
        }

        const uint32_t slot = static_cast<uint32_t>(_degrees.size());
        _slotOfKey[key] = slot;
        _degrees.push_back(degree);

        return slot;
    }

    void grow() {
        const size_t capacity = _capacity == 0 ? 16 : 2 * _capacity;
        std::vector<size_t> sums(capacity * capacity, 0);
        for (size_t low = 0; low < _capacity; low++) {
            std::copy_n(_sums.begin() + low * _capacity, _capacity, sums.begin() + low * capacity);
        }

        _sums.swap(sums);
        _capacity = capacity;
    }
};

void sortAndCombine(std::vector<DegreeProduct>& products) {
    std::ranges::sort(products, std::less<ProductTriple> {}, tripleOf);

    size_t kept = 0;
    for (const DegreeProduct& product : products) {
        if (kept != 0 && tripleOf(products[kept - 1]) == tripleOf(product)) {
            products[kept - 1]._sum += product._sum;
        } else {
            products[kept] = product;
            kept++;
        }
    }

    products.resize(kept);
}

size_t findOwnerIndex(DataPartSpan parts, NodeID nodeID) {
    const auto firstNodeOf = [](const WeakArc<DataPart>& part) {
        return part->getFirstNodeID();
    };

    const auto after = std::ranges::upper_bound(parts, nodeID, std::less<NodeID> {}, firstNodeOf);
    bioassert(after != parts.begin(), "A node a part adds edges to precedes every earlier part");

    return static_cast<size_t>(after - parts.begin()) - 1;
}

}

DegreeProducts::DegreeProducts()
{
}

DegreeProducts::~DegreeProducts() {
}

std::unique_ptr<DegreeProducts> DegreeProducts::create(const EdgeContainer& edges,
                                                       const EdgeIndexer& indexer,
                                                       const NodeContainer& nodes,
                                                       DataPartSpan earlierParts) {
    Profile profile("DegreeProducts::create");

    const std::span<const EdgeRecord> outs = edges.getOuts();
    const std::span<const EdgeRecord> ins = edges.getIns();
    const std::span<const NodeEdgeData> nodeData = indexer.getNodeData();
    const size_t patchNodeCount = indexer.getPatchNodeCount();
    const NodeID firstNodeID = indexer.getFirstNodeID();

    const auto outsOf = [&outs](const NodeEdgeData& data) {
        return outs.subspan(data._outRange._first, data._outRange._count);
    };

    const auto insOf = [&ins](const NodeEdgeData& data) {
        return ins.subspan(data._inRange._first, data._inRange._count);
    };

    std::vector<DegreeProduct> products;
    std::vector<DegreeCount> degrees;
    ProductTally tally;

    for (const auto& [labelSet, range] : nodes.getLabelSetIndexer()) {
        for (const NodeID nodeID : range) {
            const NodeEdgeData& data = nodeData[patchNodeCount + (nodeID - firstNodeID).getValue()];

            degrees.clear();
            countAdded(outsOf(data), true, degrees);
            countAdded(insOf(data), false, degrees);
            tally.add(degrees);
        }

        tally.flush(labelSet, products);
    }

    // The earlier nodes the part adds edges to, each with its label set and the parts its
    // earlier edges are in: its own, and the later ones holding patches of earlier nodes
    struct EarlierNode {
        NodeID _nodeID;
        LabelSetHandle _labelSet;
        size_t _ownerIndex {0};
        size_t _dataIndex {0};
    };

    std::vector<size_t> patchingParts;
    for (size_t partIndex = 0; partIndex < earlierParts.size(); partIndex++) {
        if (earlierParts[partIndex]->edgeIndexer().getPatchNodeCount() > 0) {
            patchingParts.push_back(partIndex);
        }
    }

    std::vector<EarlierNode> earlierNodes;
    for (size_t dataIndex = 0; dataIndex < patchNodeCount; dataIndex++) {
        const NodeEdgeData& data = nodeData[dataIndex];
        const NodeID nodeID = data._outRange._count != 0 ? outs[data._outRange._first]._nodeID
                                                         : ins[data._inRange._first]._nodeID;

        const size_t ownerIndex = findOwnerIndex(earlierParts, nodeID);
        const LabelSetHandle labelSet = earlierParts[ownerIndex]->nodes().getNodeLabelSet(nodeID);

        earlierNodes.push_back(EarlierNode {._nodeID = nodeID,
                                            ._labelSet = labelSet,
                                            ._ownerIndex = ownerIndex,
                                            ._dataIndex = dataIndex});
    }

    std::ranges::sort(earlierNodes, {}, [](const EarlierNode& node) {
        return node._labelSet.getID().getValue();
    });

    for (size_t i = 0; i < earlierNodes.size(); i++) {
        const EarlierNode& node = earlierNodes[i];
        const NodeEdgeData& data = nodeData[node._dataIndex];

        degrees.clear();
        countAdded(outsOf(data), true, degrees);
        countAdded(insOf(data), false, degrees);

        const EdgeIndexer& ownerIndexer = earlierParts[node._ownerIndex]->edgeIndexer();
        countBefore(ownerIndexer.getNodeOutEdges(node._nodeID), true, degrees);
        countBefore(ownerIndexer.getNodeInEdges(node._nodeID), false, degrees);

        const auto firstPatching = std::ranges::upper_bound(patchingParts, node._ownerIndex);
        for (const size_t partIndex : std::span(firstPatching, patchingParts.end())) {
            const EdgeIndexer& patchIndexer = earlierParts[partIndex]->edgeIndexer();
            countBefore(patchIndexer.getNodeOutEdges(node._nodeID), true, degrees);
            countBefore(patchIndexer.getNodeInEdges(node._nodeID), false, degrees);
        }

        tally.add(degrees);

        const bool endsLabelSet = i + 1 == earlierNodes.size()
                               || earlierNodes[i + 1]._labelSet.getID() != node._labelSet.getID();
        if (endsLabelSet) {
            tally.flush(node._labelSet, products);
        }
    }

    std::unique_ptr<DegreeProducts> degreeProducts = std::make_unique<DegreeProducts>();
    degreeProducts->assign(products);

    return degreeProducts;
}

void DegreeProducts::assign(std::span<const DegreeProduct> products) {
    _products.assign(products.begin(), products.end());
    sortAndCombine(_products);
}

DegreeProductIndex::DegreeProductIndex()
{
}

DegreeProductIndex::~DegreeProductIndex() {
}

void DegreeProductIndex::build(std::span<const DegreeProduct> products) {
    _entries.clear();
    _labelSets.clear();

    size_t begin = 0;
    for (size_t i = 0; i < products.size(); i++) {
        const DegreeProduct& product = products[i];
        _entries.push_back(Entry {._first = product._first, ._second = product._second, ._sum = product._sum});
        if (keyOf(product._first) != keyOf(product._second)) {
            _entries.push_back(Entry {._first = product._second, ._second = product._first, ._sum = product._sum});
        }

        const bool endsLabelSet = i + 1 == products.size()
                               || products[i + 1]._labelSet.getID() != product._labelSet.getID();
        if (!endsLabelSet) {
            continue;
        }

        const auto labelSetEntries = std::span(_entries).subspan(begin);
        std::ranges::sort(labelSetEntries, {}, [](const Entry& entry) {
            return std::tuple {sideKeyOf(entry._first), sideKeyOf(entry._second)};
        });

        _labelSets.push_back(LabelSetEntries {._labelSet = product._labelSet, ._begin = begin, ._end = _entries.size()});
        begin = _entries.size();
    }
}

size_t DegreeProductIndex::sumProducts(const LabelSetHandle& labels,
                                       const DegreeSelection& first,
                                       const DegreeSelection& second) const {
    const auto sideOf = [](const Entry& entry) {
        return entry._first._isOut;
    };

    const auto firstKeyOf = [](const Entry& entry) {
        return sideKeyOf(entry._first);
    };

    size_t sum = 0;
    const auto addSelected = [&sum, &second](std::span<const Entry> entries) {
        for (const Entry& entry : entries) {
            if (selects(second, entry._second)) {
                sum += entry._sum;
            }
        }
    };

    for (const LabelSetEntries& labelSet : _labelSets) {
        if (!labelSet._labelSet.hasAtLeastLabels(labels)) {
            continue;
        }

        const std::span<const Entry> entries = std::span(_entries).subspan(labelSet._begin, labelSet._end - labelSet._begin);
        if (first._isEveryType) {
            addSelected(std::ranges::equal_range(entries, first._isOut, std::greater<bool> {}, sideOf));
            continue;
        }

        for (const EdgeTypeID edgeType : first._edgeTypes) {
            const TypedDegree degree {._edgeTypeID = edgeType, ._isOut = first._isOut};
            addSelected(std::ranges::equal_range(entries, sideKeyOf(degree), {}, firstKeyOf));
        }
    }

    return sum;
}
