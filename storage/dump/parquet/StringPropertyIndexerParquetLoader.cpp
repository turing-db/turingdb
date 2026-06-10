#include "StringPropertyIndexerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "ParquetFileReading.h"
#include "StringPropertyIndexerParquetLayout.h"

#include "indexers/StringPropertyIndexer.h"
#include "indexes/StringIndex.h"
#include "Path.h"

#include "ID.h"
#include "FatalException.h"

using namespace db;

namespace layout = stringPropertyIndexerParquetLayout;

namespace {

// Indexes table: property_type_id, node_count.
struct IndexesData {
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _nodeCounts;
};

// Fills the caller-owned IndexesData from the two columns.
class IndexesVisitor : public ParquetSaxVisitor {
public:
    explicit IndexesVisitor(IndexesData& data)
        : _data(data) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = (columnIndex == 0) ? _data._propertyTypeIds : _data._nodeCounts;
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    IndexesData& _data;
};

// Children table: property_type_id, parent_node_id, child_index, child_node_id.
struct ChildrenData {
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _parentIds;
    std::vector<int64_t> _childIndices;
    std::vector<int64_t> _childIds;
};

// Fills the caller-owned ChildrenData, routing each column by its index.
class ChildrenVisitor : public ParquetSaxVisitor {
public:
    explicit ChildrenVisitor(ChildrenData& data)
        : _data(data) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    ChildrenData& _data;

    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _data._propertyTypeIds;
        } else if (columnIndex == 1) {
            return _data._parentIds;
        } else if (columnIndex == 2) {
            return _data._childIndices;
        } else if (columnIndex == 3) {
            return _data._childIds;
        } else {
            throw FatalException("StringPropertyIndexerParquetLoader: unexpected children column");
        }
    }
};

// Owners table: property_type_id, node_id, entity_id.
struct OwnersData {
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _nodeIds;
    std::vector<int64_t> _entityIds;
};

// Fills the caller-owned OwnersData, routing each column by its index.
class OwnersVisitor : public ParquetSaxVisitor {
public:
    explicit OwnersVisitor(OwnersData& data)
        : _data(data) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    OwnersData& _data;

    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _data._propertyTypeIds;
        } else if (columnIndex == 1) {
            return _data._nodeIds;
        } else if (columnIndex == 2) {
            return _data._entityIds;
        } else {
            throw FatalException("StringPropertyIndexerParquetLoader: unexpected owners column");
        }
    }
};

void checkColumnsAgree(bool columnsAgree, std::string_view table) {
    if (!columnsAgree) {
        throw FatalException(fmt::format(
            "StringPropertyIndexerParquetLoader: {} columns have mismatched lengths", table));
    }
}

// The children/owners rows reference indexes and tree nodes created from the indexes
// table; a corrupt file can name either one that does not exist, so resolve with a
// clean error instead of letting std::out_of_range escape.
StringIndex* resolveIndex(const std::unordered_map<int64_t, StringIndex*>& indexByPropertyTypeId,
                          int64_t propertyTypeId) {
    const auto it = indexByPropertyTypeId.find(propertyTypeId);
    if (it == indexByPropertyTypeId.end()) {
        throw FatalException(fmt::format(
            "StringPropertyIndexerParquetLoader: unknown property type id {}", propertyTypeId));
    }
    return it->second;
}

StringIndex::PrefixTreeNode* nodeAt(const StringIndex& index, int64_t rawNodeId) {
    const size_t nodeId = static_cast<size_t>(rawNodeId);
    if (nodeId >= index.getNodeCount()) {
        throw FatalException(fmt::format(
            "StringPropertyIndexerParquetLoader: node id {} out of range ({} nodes)",
            nodeId, index.getNodeCount()));
    }
    return index.getNode(nodeId);
}

}

std::unique_ptr<StringPropertyIndexer> StringPropertyIndexerParquetLoader::load(
    const fs::Path& indexesPath,
    const fs::Path& childrenPath,
    const fs::Path& ownersPath) {
    IndexesData indexes;
    {
        IndexesVisitor visitor(indexes);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::NODE_COUNT_COLUMN, ParquetColumnType::UInt64);
        readParquetFile(indexesPath, visitor, expectedSchema);
    }

    ChildrenData children;
    {
        ChildrenVisitor visitor(children);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::PARENT_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::CHILD_INDEX_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::CHILD_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        readParquetFile(childrenPath, visitor, expectedSchema);
    }

    OwnersData owners;
    {
        OwnersVisitor visitor(owners);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::NODE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::ENTITY_ID_COLUMN, ParquetColumnType::UInt64);
        readParquetFile(ownersPath, visitor, expectedSchema);
    }

    checkColumnsAgree(indexes._nodeCounts.size() == indexes._propertyTypeIds.size(),
                      "indexes");
    checkColumnsAgree(children._parentIds.size() == children._propertyTypeIds.size()
                          && children._childIndices.size() == children._propertyTypeIds.size()
                          && children._childIds.size() == children._propertyTypeIds.size(),
                      "children");
    checkColumnsAgree(owners._nodeIds.size() == owners._propertyTypeIds.size()
                          && owners._entityIds.size() == owners._propertyTypeIds.size(),
                      "owners");

    auto indexer = std::make_unique<StringPropertyIndexer>();

    // Create each index with its node count (pre-allocates dense nodes), keeping a
    // raw pointer keyed by the serialized property-type id for the link/owner passes.
    std::unordered_map<int64_t, StringIndex*> indexByPropertyTypeId;
    for (size_t i = 0; i < indexes._propertyTypeIds.size(); ++i) {
        const int64_t rawPropertyTypeId = indexes._propertyTypeIds[i];
        const size_t nodeCount = static_cast<size_t>(indexes._nodeCounts[i]);

        // A StringIndex always holds at least its root; zero can only come from a
        // corrupt file (and would underflow the index's pre-allocation).
        if (nodeCount == 0) {
            throw FatalException("StringPropertyIndexerParquetLoader: index has zero nodes");
        }

        auto index = std::make_unique<StringIndex>(nodeCount);
        StringIndex* raw = index.get();
        const PropertyTypeID propertyTypeID {static_cast<PropertyTypeID::Type>(rawPropertyTypeId)};
        indexer->addIndex(propertyTypeID, std::move(index));
        indexByPropertyTypeId[rawPropertyTypeId] = raw;
    }

    for (size_t i = 0; i < children._propertyTypeIds.size(); ++i) {
        const StringIndex* index = resolveIndex(indexByPropertyTypeId,
                                                children._propertyTypeIds[i]);
        StringIndex::PrefixTreeNode* parent = nodeAt(*index, children._parentIds[i]);
        StringIndex::PrefixTreeNode* child = nodeAt(*index, children._childIds[i]);

        const size_t childIndex = static_cast<size_t>(children._childIndices[i]);
        if (childIndex >= StringIndex::PrefixTreeNode::ALPHABET_SIZE) {
            throw FatalException(fmt::format(
                "StringPropertyIndexerParquetLoader: child index {} out of range", childIndex));
        }

        parent->setChild(child, childIndex);
    }

    for (size_t i = 0; i < owners._propertyTypeIds.size(); ++i) {
        const StringIndex* index = resolveIndex(indexByPropertyTypeId,
                                                owners._propertyTypeIds[i]);
        StringIndex::PrefixTreeNode* node = nodeAt(*index, owners._nodeIds[i]);
        node->addOwner(EntityID {static_cast<uint64_t>(owners._entityIds[i])});
    }

    indexer->setInitialised();
    return indexer;
}
