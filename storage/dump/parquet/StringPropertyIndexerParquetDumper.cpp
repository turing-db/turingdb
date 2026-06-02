#include "StringPropertyIndexerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "indexers/StringPropertyIndexer.h"
#include "indexes/StringIndex.h"
#include "Path.h"

using namespace db;

namespace {

constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view NODE_COUNT_COLUMN = "node_count";
constexpr std::string_view PARENT_NODE_ID_COLUMN = "parent_node_id";
constexpr std::string_view CHILD_INDEX_COLUMN = "child_index";
constexpr std::string_view CHILD_NODE_ID_COLUMN = "child_node_id";
constexpr std::string_view NODE_ID_COLUMN = "node_id";
constexpr std::string_view ENTITY_ID_COLUMN = "entity_id";

}

void StringPropertyIndexerParquetDumper::dump(const StringPropertyIndexer& indexer,
                                              const fs::Path& indexesPath,
                                              const fs::Path& childrenPath,
                                              const fs::Path& ownersPath) {
    // Indexes table: one row per indexed property type.
    {
        ParquetWriteSchema schema;
        schema.addColumn(PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(NODE_COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(indexesPath, schema);

        std::vector<int64_t> propertyTypeIds;
        std::vector<int64_t> nodeCounts;
        for (const auto& [propertyTypeID, index] : indexer) {
            propertyTypeIds.push_back(static_cast<int64_t>(propertyTypeID.getValue()));
            nodeCounts.push_back(static_cast<int64_t>(index->getNodeCount()));
        }

        if (!propertyTypeIds.empty()) {
            writer.beginRowGroup(propertyTypeIds.size());
            writer.writeInt64Column(0, propertyTypeIds);
            writer.writeInt64Column(1, nodeCounts);
        }

        writer.finish();
    }

    // Children table: one row per non-null child link.
    {
        ParquetWriteSchema schema;
        schema.addColumn(PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(PARENT_NODE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(CHILD_INDEX_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(CHILD_NODE_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(childrenPath, schema);

        std::vector<int64_t> propertyTypeIds;
        std::vector<int64_t> parentIds;
        std::vector<int64_t> childIndices;
        std::vector<int64_t> childIds;
        for (const auto& [propertyTypeID, index] : indexer) {
            const size_t nodeCount = index->getNodeCount();
            for (size_t i = 0; i < nodeCount; ++i) {
                const StringIndex::PrefixTreeNode* node = index->getNode(i);
                const std::vector<StringIndex::PrefixTreeNode*>& children = node->getChildren();
                for (size_t childIndex = 0; childIndex < children.size(); ++childIndex) {
                    const StringIndex::PrefixTreeNode* child = children[childIndex];
                    if (child != nullptr) {
                        propertyTypeIds.push_back(static_cast<int64_t>(propertyTypeID.getValue()));
                        parentIds.push_back(static_cast<int64_t>(node->getID()));
                        childIndices.push_back(static_cast<int64_t>(childIndex));
                        childIds.push_back(static_cast<int64_t>(child->getID()));
                    }
                }
            }
        }

        if (!propertyTypeIds.empty()) {
            writer.beginRowGroup(propertyTypeIds.size());
            writer.writeInt64Column(0, propertyTypeIds);
            writer.writeInt64Column(1, parentIds);
            writer.writeInt64Column(2, childIndices);
            writer.writeInt64Column(3, childIds);
        }

        writer.finish();
    }

    // Owners table: one row per (node, owner) pair.
    {
        ParquetWriteSchema schema;
        schema.addColumn(PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(NODE_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(ENTITY_ID_COLUMN, ParquetColumnType::UInt64);

        ParquetWriter writer(ownersPath, schema);

        std::vector<int64_t> propertyTypeIds;
        std::vector<int64_t> nodeIds;
        std::vector<int64_t> entityIds;
        for (const auto& [propertyTypeID, index] : indexer) {
            const size_t nodeCount = index->getNodeCount();
            for (size_t i = 0; i < nodeCount; ++i) {
                const StringIndex::PrefixTreeNode* node = index->getNode(i);
                for (const EntityID owner : node->getOwners()) {
                    propertyTypeIds.push_back(static_cast<int64_t>(propertyTypeID.getValue()));
                    nodeIds.push_back(static_cast<int64_t>(node->getID()));
                    entityIds.push_back(static_cast<int64_t>(owner.getValue()));
                }
            }
        }

        if (!propertyTypeIds.empty()) {
            writer.beginRowGroup(propertyTypeIds.size());
            writer.writeInt64Column(0, propertyTypeIds);
            writer.writeInt64Column(1, nodeIds);
            writer.writeInt64Column(2, entityIds);
        }

        writer.finish();
    }
}
