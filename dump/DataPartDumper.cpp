#include "DataPartDumper.h"

#include "DataPart.h"
#include "DumpConfig.h"
#include "FilePageWriter.h"
#include "Path.h"
#include "Profiler.h"
#include "StringApproxIndexerDumper.h"
#include "indexers/StringPropertyIndexer.h"
#include "properties/PropertyManager.h"
#include "DataPartInfoDumper.h"
#include "EdgeIndexerDumper.h"
#include "NodeContainerDumper.h"
#include "EdgeContainerDumper.h"
#include "PropertyContainerDumper.h"
#include "PropertyIndexerDumper.h"
#include "Panic.h"

using namespace db;

namespace {

DumpResult<void> dumpProperties(fs::FilePageWriter& writer, PropertyContainer* container) {
    switch (container->getValueType()) {
        case ValueType::UInt64: {
            TrivialPropertyContainerDumper<types::UInt64> dumper {writer};
            if (auto res = dumper.dump(container->cast<types::UInt64>()); !res) {
                return res.get_unexpected();
            }
            break;
        }
        case ValueType::Int64: {
            TrivialPropertyContainerDumper<types::Int64> dumper {writer};
            if (auto res = dumper.dump(container->cast<types::Int64>()); !res) {
                return res.get_unexpected();
            }
            break;
        }
        case ValueType::Double: {
            TrivialPropertyContainerDumper<types::Double> dumper {writer};
            if (auto res = dumper.dump(container->cast<types::Double>()); !res) {
                return res.get_unexpected();
            }
            break;
        }
        case ValueType::String: {
            StringPropertyContainerDumper dumper {writer};
            if (auto res = dumper.dump(container->cast<types::String>()); !res) {
                return res.get_unexpected();
            }
            break;
        }
        case ValueType::Bool: {
            TrivialPropertyContainerDumper<types::Bool> dumper {writer};
            if (auto res = dumper.dump(container->cast<types::Bool>()); !res) {
                return res.get_unexpected();
            }
            break;
        }
        case ValueType::_SIZE:
        case ValueType::Invalid: {
            panic("Error, invalid type");
        }
    }

    return {};
}

}

DumpResult<void> DataPartDumper::dump(const DataPart& part, const fs::Path& path) {
    Profile profile {"DataPartDumper::dump"};
    if (path.exists()) {
        return DumpError::result(DumpErrorType::DATAPART_ALREADY_EXISTS);
    }

    if (auto res = path.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_DATAPART, res.get_unexpected().error());
    }

    {
        // Dumping info
        Profile profile {"DataPartDumper::dump <info>"};
        const fs::Path infoPath = path / "info";

        auto writer = fs::FilePageWriter::open(infoPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_INFO, writer.error());
        }

        DataPartInfoDumper dumper {writer.value()};

        if (auto res = dumper.dump(part); !res) {
            return res.get_unexpected();
        }
    }

    // Dumping nodes
    const auto& nodes = part.nodes();
    if (nodes.size() != 0) {
        const fs::Path nodesPath = path / "nodes";

        auto writer = fs::FilePageWriter::open(nodesPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODES, writer.error());
        }

        NodeContainerDumper dumper {writer.value()};

        if (auto res = dumper.dump(nodes); !res) {
            return res.get_unexpected();
        }
    }

    // Dumping edges
    const auto& edges = part.edges();
    if (edges.size() != 0) {
        const fs::Path edgesPath = path / "edges";

        auto writer = fs::FilePageWriter::open(edgesPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGES, writer.error());
        }

        EdgeContainerDumper dumper {writer.value()};

        if (auto res = dumper.dump(edges); !res) {
            return res.get_unexpected();
        }
    }

    // Dumping edge indexer
    const auto& edgeIndexer = part.edgeIndexer();
    const fs::Path edgeIndexerPath = path / "edge-indexer";

    auto writer = fs::FilePageWriter::open(edgeIndexerPath, DumpConfig::PAGE_SIZE);
    if (!writer) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_INDEXER, writer.error());
    }

    EdgeIndexerDumper dumper {writer.value()};

    if (auto res = dumper.dump(edgeIndexer); !res) {
        return res.get_unexpected();
    }

    // Dumping node properties
    {
        Profile profile {"DataPartDumper::dump <node props>"};
        const auto& nodeProperties = part.nodeProperties();

        // Dumping indexer
        {
            const fs::Path indexerPath = path / "node-prop-indexer";

            auto writer = fs::FilePageWriter::open(indexerPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROP_INDEXER, writer.error());
            }

            PropertyIndexerDumper dumper {writer.value()};

            if (auto res = dumper.dump(nodeProperties.indexers()); !res) {
                return res.get_unexpected();
            }
        }

        // Dumping containers
        for (const auto& [ptID, container] : nodeProperties) {
            const fs::Path propsPath = path / "node-props-" + std::to_string(ptID);

            auto writer = fs::FilePageWriter::open(propsPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS, writer.error());
            }

            if (auto res = dumpProperties(writer.value(), container.get()); !res) {
                return res.get_unexpected();
            }
        }
    }

    // Dumping edge properties
    {
        Profile profile {"DataPartDumper::dump <edge props>"};
        const auto& edgeProperties = part.edgeProperties();

        // Dumping indexer
        {
            const fs::Path indexerPath = path / "edge-prop-indexer";

            auto writer = fs::FilePageWriter::open(indexerPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROP_INDEXER, writer.error());
            }

            PropertyIndexerDumper dumper {writer.value()};

            if (auto res = dumper.dump(edgeProperties.indexers()); !res) {
                return res.get_unexpected();
            }
        }

        for (const auto& [ptID, container] : edgeProperties) {
            const fs::Path propsPath = path / "edge-props-" + std::to_string(ptID);

            auto writer = fs::FilePageWriter::open(propsPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROPS, writer.error());
            }

            if (auto res = dumpProperties(writer.value(), container.get()); !res) {
                return res.get_unexpected();
            }
        }
    }

    // Dumping node String approx property Indexer
    {
        Profile profile {"DataPartDumper::dump <node string prop indexer>"};
        const auto& index = part.getNodeStrPropIndexer();

        {
            const fs::Path strIndexerPath = path / "node-string-prop-indexer";
            auto writer = fs::FilePageWriter::open(strIndexerPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(
                    DumpErrorType::CANNOT_OPEN_DATAPART_NODE_STR_PROP_INDEXER,
                    writer.error());
            }

            StringApproxIndexerDumper dumper {writer.value()};

            if (auto res = dumper.dump(index); !res) {
                return res.get_unexpected();
            }
        }
    }


    // Dumping edge String approx property Indexer
    {
        Profile profile {"DataPartDumper::dump <edge string prop indexer>"};
        const auto& index = part.getEdgeStrPropIndexer();

        {
            const fs::Path strIndexerPath = path / "edge-string-prop-indexer";
            auto writer = fs::FilePageWriter::open(strIndexerPath, DumpConfig::PAGE_SIZE);
            if (!writer) {
                return DumpError::result(
                    DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_STR_PROP_INDEXER,
                    writer.error());
            }

            StringApproxIndexerDumper dumper {writer.value()};

            if (auto res = dumper.dump(index); !res) {
                return res.get_unexpected();
            }
        }
    }

    return {};
}
