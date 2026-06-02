#include "StringPropertyIndexerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include "ParquetReader.h"

#include "indexers/StringPropertyIndexer.h"
#include "indexes/StringIndex.h"
#include "Path.h"

#include "ID.h"

using namespace db;

namespace {

// Indexes table: property_type_id, node_count.
class IndexesVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _nodeCounts;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = (columnIndex == 0) ? _propertyTypeIds : _nodeCounts;
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }
};

// Children table: property_type_id, parent_node_id, child_index, child_node_id.
class ChildrenVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _parentIds;
    std::vector<int64_t> _childIndices;
    std::vector<int64_t> _childIds;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _propertyTypeIds;
        } else if (columnIndex == 1) {
            return _parentIds;
        } else if (columnIndex == 2) {
            return _childIndices;
        } else {
            return _childIds;
        }
    }
};

// Owners table: property_type_id, node_id, entity_id.
class OwnersVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _nodeIds;
    std::vector<int64_t> _entityIds;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _propertyTypeIds;
        } else if (columnIndex == 1) {
            return _nodeIds;
        } else {
            return _entityIds;
        }
    }
};

template <typename Visitor>
void readFile(const fs::Path& path, Visitor& visitor) {
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
}

}

std::unique_ptr<StringPropertyIndexer> StringPropertyIndexerParquetLoader::load(
    const fs::Path& indexesPath,
    const fs::Path& childrenPath,
    const fs::Path& ownersPath) {
    IndexesVisitor indexesVisitor;
    readFile(indexesPath, indexesVisitor);

    ChildrenVisitor childrenVisitor;
    readFile(childrenPath, childrenVisitor);

    OwnersVisitor ownersVisitor;
    readFile(ownersPath, ownersVisitor);

    auto indexer = std::make_unique<StringPropertyIndexer>();

    // Create each index with its node count (pre-allocates dense nodes), keeping a
    // raw pointer keyed by the serialized property-type id for the link/owner passes.
    std::unordered_map<int64_t, StringIndex*> indexByPropertyTypeId;
    for (size_t i = 0; i < indexesVisitor._propertyTypeIds.size(); ++i) {
        const int64_t rawPropertyTypeId = indexesVisitor._propertyTypeIds[i];
        const size_t nodeCount = static_cast<size_t>(indexesVisitor._nodeCounts[i]);

        auto index = std::make_unique<StringIndex>(nodeCount);
        StringIndex* raw = index.get();
        const PropertyTypeID propertyTypeID {static_cast<PropertyTypeID::Type>(rawPropertyTypeId)};
        indexer->addIndex(propertyTypeID, std::move(index));
        indexByPropertyTypeId[rawPropertyTypeId] = raw;
    }

    for (size_t i = 0; i < childrenVisitor._propertyTypeIds.size(); ++i) {
        StringIndex* index = indexByPropertyTypeId.at(childrenVisitor._propertyTypeIds[i]);
        StringIndex::PrefixTreeNode* parent =
            index->getNode(static_cast<size_t>(childrenVisitor._parentIds[i]));
        StringIndex::PrefixTreeNode* child =
            index->getNode(static_cast<size_t>(childrenVisitor._childIds[i]));
        parent->setChild(child, static_cast<size_t>(childrenVisitor._childIndices[i]));
    }

    for (size_t i = 0; i < ownersVisitor._propertyTypeIds.size(); ++i) {
        StringIndex* index = indexByPropertyTypeId.at(ownersVisitor._propertyTypeIds[i]);
        StringIndex::PrefixTreeNode* node =
            index->getNode(static_cast<size_t>(ownersVisitor._nodeIds[i]));
        node->addOwner(EntityID {static_cast<uint64_t>(ownersVisitor._entityIds[i])});
    }

    indexer->setInitialised();
    return indexer;
}
