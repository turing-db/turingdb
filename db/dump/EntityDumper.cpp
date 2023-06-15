#include "EntityDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "MsgCommon.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "Writeback.h"
#include "capnp/EntityIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <functional>
#include <unistd.h>

namespace db {

EntityDumper::EntityDumper(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db) {
}

bool EntityDumper::dump() {
    // Remove entities file if it already exists
    if (FileUtils::exists(_indexPath)) {
        if (!FileUtils::removeFile(_indexPath)) {
            Log::BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE() << _indexPath);
            return false;
        }
    }

    int indexFD = FileUtils::openForWrite(_indexPath);
    if (indexFD < 0) {
        return false;
    }

    DB::NetworkRange networks = _db->networks();

    ::capnp::MallocMessageBuilder message;
    auto entities = message.initRoot<OnDisk::EntityIndex>();
    auto networksBuilder = entities.initNetworks(networks.size());

    size_t i = 0;
    for (const Network* net : networks) {
        // Nodes
        Network::NodeRange nodes = net->nodes();

        auto diskNetwork = networksBuilder[i];
        diskNetwork.setNameId(net->getName().getID());
        auto nodesBuilder = diskNetwork.initNodes(nodes.size());

        size_t j = 0;
        for (const Node* node : nodes) {
            auto diskNode = nodesBuilder[j];
            auto props = node->properties();
            auto diskProperties = diskNode.initProperties(props.size());

            diskNode.setId(j);
            diskNode.setNameId(node->getName().getID());
            diskNode.setNodeTypeNameId(node->getType()->getName().getID());

            size_t k = 0;
            for (const auto& [propType, propValue] : props) {
                auto diskProp = diskProperties[k];
                const auto kind = propType->getValueType().getKind();
                const auto diskKind = static_cast<OnDisk::ValueKind>(kind);
                diskProp.setKind(diskKind);
                diskProp.setPropertyTypeNameId(propType->getName().getID());

                switch (kind) {
                    case ValueType::VK_INT: {
                        diskProp.getValue().setInt(propValue.getInt());
                        break;
                    }
                    case ValueType::VK_UNSIGNED: {
                        diskProp.getValue().setUnsigned(propValue.getUint());
                        break;
                    }
                    case ValueType::VK_BOOL: {
                        diskProp.getValue().setBool(propValue.getBool());
                        break;
                    }
                    case ValueType::VK_DECIMAL: {
                        diskProp.getValue().setDecimal(propValue.getDouble());
                        break;
                    }
                    case ValueType::VK_STRING: {
                        diskProp.getValue().setString(propValue.getString());
                        break;
                    }
                    case ValueType::VK_STRING_REF: {
                        diskProp.getValue().setStringRefId(
                            propValue.getStringRef().getID());
                        diskProp.getValue().setStringRefId(0);
                        break;
                    }
                    case ValueType::VK_INVALID: {
                        Log::BioLog::echo(
                            "[FATAL ERROR, SHOULD NOT OCCUR] An invalid "
                            "property was encountered when dumping database");
                        break;
                    }
                    case ValueType::_SIZE: {
                        Log::BioLog::echo(
                            "[FATAL ERROR, SHOULD NOT OCCUR] An invalid "
                            "property was encountered when dumping database");
                        break;
                    }
                }
                k++;
            }
            j++;
        }

        // Edges
        Network::EdgeRange edges = net->edges();

        auto edgesBuilder = diskNetwork.initEdges(edges.size());

        j = 0;
        for (const Edge* edge : edges) {
            auto diskEdge = edgesBuilder[j];
            auto props = edge->properties();
            auto diskProperties = diskEdge.initProperties(props.size());

            diskEdge.setId(j);
            diskEdge.setEdgeTypeNameId(edge->getType()->getName().getID());
            diskEdge.setSourceId(edge->getSource()->getIndex());
            diskEdge.setTargetId(edge->getTarget()->getIndex());

            size_t k = 0;
            for (const auto& [propType, propValue] : props) {
                auto diskProp = diskProperties[k];
                const auto kind = propType->getValueType().getKind();
                const auto diskKind = static_cast<OnDisk::ValueKind>(kind);
                diskProp.setKind(diskKind);
                diskProp.setPropertyTypeNameId(propType->getName().getID());

                switch (kind) {
                    case ValueType::VK_INT: {
                        diskProp.getValue().setInt(propValue.getInt());
                        break;
                    }
                    case ValueType::VK_UNSIGNED: {
                        diskProp.getValue().setUnsigned(propValue.getUint());
                        break;
                    }
                    case ValueType::VK_BOOL: {
                        diskProp.getValue().setBool(propValue.getBool());
                        break;
                    }
                    case ValueType::VK_DECIMAL: {
                        diskProp.getValue().setDecimal(propValue.getDouble());
                        break;
                    }
                    case ValueType::VK_STRING: {
                        diskProp.getValue().setString(propValue.getString());
                        break;
                    }
                    case ValueType::VK_STRING_REF: {
                        diskProp.getValue().setStringRefId(
                            propValue.getStringRef().getID());
                        break;
                    }
                    case ValueType::VK_INVALID: {
                        Log::BioLog::echo(
                            "[FATAL ERROR, SHOULD NOT OCCUR] An invalid "
                            "property was encountered when dumping database");
                        break;
                    }
                    case ValueType::_SIZE: {
                        Log::BioLog::echo(
                            "[FATAL ERROR, SHOULD NOT OCCUR] An invalid "
                            "property was encountered when dumping database");
                        break;
                    }
                }
                k++;
            }
            j++;
        }
        i++;
    }

    // Node
    ::capnp::writePackedMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}

}
