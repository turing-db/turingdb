#include "DataPartLoader.h"

#include "DataPart.h"
#include "GraphMetadata.h"
#include "Graph.h"
#include "properties/PropertyManager.h"
#include "indexers/EdgeIndexer.h"
#include "DataPartInfoLoader.h"
#include "EdgeIndexerLoader.h"
#include "NodeContainerLoader.h"
#include "EdgeContainerLoader.h"
#include "PropertyContainerLoader.h"
#include "PropertyIndexerLoader.h"
#include "versioning/VersionController.h"
#include "Panic.h"

using namespace db;

DumpResult<WeakArc<const DataPart>> DataPartLoader::load(const fs::Path& path,
                                                         const GraphMetadata& metadata,
                                                         VersionController& versionController) {
    if (!path.exists()) {
        return DumpError::result(DumpErrorType::DATAPART_DOES_NOT_EXIST);
    }

    WeakArc<DataPart> part = versionController.createDataPart(0, 0);

    // Loading info
    {
        const fs::Path infoPath = path / "info";
        auto reader = fs::FilePageReader::open(infoPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_INFO, reader.error());
        }

        DataPartInfoLoader loader {reader.value()};

        auto res = loader.load(*part);
        if (!res) {
            return res.get_unexpected();
        }
    }

    // Loading nodes
    const fs::Path nodesPath = path / "nodes";
    if (nodesPath.exists()) {
        auto reader = fs::FilePageReader::open(nodesPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODES, reader.error());
        }

        NodeContainerLoader loader {reader.value()};

        auto res = loader.load(metadata);
        if (!res) {
            return res.get_unexpected();
        }

        part->_nodes = std::move(res.value());
    } else {
        auto* ptr = new NodeContainer(part->_firstNodeID, 0, metadata);
        part->_nodes = std::unique_ptr<NodeContainer> {ptr};
    }

    // Loading edges
    const fs::Path edgesPath = path / "edges";
    if (edgesPath.exists()) {
        auto reader = fs::FilePageReader::open(edgesPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGES, reader.error());
        }

        EdgeContainerLoader loader {reader.value()};

        auto res = loader.load(metadata);
        if (!res) {
            return res.get_unexpected();
        }

        part->_edges = std::move(res.value());
        part->_firstEdgeID = part->_edges->getFirstEdgeID();
    } else {
        auto* ptr = new EdgeContainer(part->_firstEdgeID, part->_firstEdgeID, {}, {});
        part->_edges = std::unique_ptr<EdgeContainer> {ptr};
    }

    // Loading edge indexer
    const fs::Path edgeIndexerPath = path / "edge-indexer";
    if (edgeIndexerPath.exists()) {
        auto reader = fs::FilePageReader::open(edgeIndexerPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_INDEXER, reader.error());
        }

        EdgeIndexerLoader loader {reader.value()};

        auto res = loader.load(metadata, *part->_edges);
        if (!res) {
            return res.get_unexpected();
        }

        part->_edgeIndexer = std::move(res.value());
    }

    // Listing files in the folder
    auto files = path.listDir();
    if (!files) {
        return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPART_FILES, files.error());
    }

    part->_nodeProperties = std::make_unique<PropertyManager>(&metadata);
    part->_edgeProperties = std::make_unique<PropertyManager>(&metadata);

    // Loading properties
    const auto loadProperties = [&](PropertyManager& manager, std::string_view filename) -> DumpResult<void> {
        const auto ptID = GraphDumpHelper::getIntegerSuffix(filename, PREFIX_SIZE);
        if (!ptID) {
            return DumpError::result(DumpErrorType::INCORRECT_PROPERTY_TYPE_ID);
        }

        auto reader = fs::FilePageReader::open(path / filename);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS, reader.error());
        }

        const PropertyType pt = metadata.propTypes().get(ptID.value());

        // Lambda to store trivial properties
        const auto storeTrivialContainer = [&]<TrivialSupportedType T>(PropertyManager& manager) -> DumpResult<void> {
            TrivialPropertyContainerLoader<T> loader {reader.value()};

            auto props = loader.load();
            if (!props) {
                return props.get_unexpected();
            }

            auto* ptr = props.value().release();
            manager._map.emplace(pt._id, static_cast<PropertyContainer*>(ptr));

            if constexpr (std::is_same_v<T, types::UInt64>) {
                manager._uint64s.emplace(pt._id, static_cast<PropertyContainer*>(ptr));
            } else if constexpr (std::is_same_v<T, types::Int64>) {
                manager._int64s.emplace(pt._id, static_cast<PropertyContainer*>(ptr));
            } else if constexpr (std::is_same_v<T, types::Double>) {
                manager._doubles.emplace(pt._id, static_cast<PropertyContainer*>(ptr));
            } else if constexpr (std::is_same_v<T, types::Bool>) {
                manager._bools.emplace(pt._id, static_cast<PropertyContainer*>(ptr));
            }

            return {};
        };

        // Lambda to store string properties
        const auto storeStringContainer = [&](PropertyManager& manager) -> DumpResult<void> {
            StringPropertyContainerLoader loader {reader.value()};

            auto props = loader.load();
            if (!props) {
                return props.get_unexpected();
            }

            auto* ptr = props.value().release();
            manager._map.emplace(pt._id, static_cast<PropertyContainer*>(ptr));

            manager._strings.emplace(pt._id, static_cast<PropertyContainer*>(ptr));

            return {};
        };

        switch (pt._valueType) {
            case ValueType::UInt64: {
                if (auto res = storeTrivialContainer.operator()<types::UInt64>(manager); !res) {
                    return res.get_unexpected();
                }
                break;
            }
            case ValueType::Int64: {
                if (auto res = storeTrivialContainer.operator()<types::Int64>(manager); !res) {
                    return res.get_unexpected();
                }
                break;
            }
            case ValueType::Double: {
                if (auto res = storeTrivialContainer.operator()<types::Double>(manager); !res) {
                    return res.get_unexpected();
                }
                break;
            }
            case ValueType::String: {
                if (auto res = storeStringContainer(manager); !res) {
                    return res.get_unexpected();
                }
                break;
            }
            case ValueType::Bool: {
                if (auto res = storeTrivialContainer.operator()<types::Bool>(manager); !res) {
                    return res.get_unexpected();
                }
                break;
            }
            case ValueType::Invalid:
            case ValueType::_SIZE:
                panic("Invalid value type");
        }

        return {};
    };

    // Loading node property indexer
    const fs::Path nodePropertyIndexerPath = path / "node-prop-indexer";

    if (nodePropertyIndexerPath.exists()) {
        auto reader = fs::FilePageReader::open(nodePropertyIndexerPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROP_INDEXER, reader.error());
        }

        PropertyIndexerLoader loader {reader.value()};

        auto res = loader.load(metadata, part->_nodeProperties->_indexers);
        if (!res) {
            return res.get_unexpected();
        }
    }

    // Loading edge property indexer
    const fs::Path edgePropertyIndexerPath = path / "edge-prop-indexer";

    if (edgePropertyIndexerPath.exists()) {
        auto reader = fs::FilePageReader::open(edgePropertyIndexerPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROP_INDEXER, reader.error());
        }

        PropertyIndexerLoader loader {reader.value()};

        auto res = loader.load(metadata, part->_edgeProperties->_indexers);
        if (!res) {
            return res.get_unexpected();
        }
    }

    for (auto& child : files.value()) {
        const auto& childStr = child.get();

        if (childStr.find(NODE_PROPS_PREFIX) != std::string::npos) {
            // node properties
            if (auto res = loadProperties(*part->_nodeProperties, childStr); !res) {
                return res.get_unexpected();
            }
        } else if (childStr.find(EDGE_PROPS_PREFIX) != std::string::npos) {
            // edge properties
            if (auto res = loadProperties(*part->_edgeProperties, childStr); !res) {
                return res.get_unexpected();
            }
        }
    }

    part->_initialized = true;

    return part;
}
