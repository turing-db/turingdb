#include "EntityDumper.h"
#include "BioAssert.h"
#include "BioLog.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "Writeback.h"
#include "capnp/EntityIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <functional>
#include <unistd.h>

namespace db {
static constexpr inline size_t entityCountLimit = 100000;

EntityDumper::EntityDumper(const db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db)
{
}

bool EntityDumper::dump() {
    // Remove data file if it already exists
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
        auto diskNetwork = networksBuilder[i];
        const auto& nodes = net->_nodes;
        const auto& edges = net->_edges;
        const size_t nodeCount = nodes.size();
        const size_t edgeCount = edges.size();
        size_t nodeCountLeft = nodeCount;
        size_t edgeCountLeft = edgeCount;

        const size_t nodeModulo = (size_t)(entityCountLimit % nodeCountLeft != 0);
        const size_t nodeSpanCount = nodeCountLeft / entityCountLimit
                                   + nodeModulo;

        const size_t edgeModulo = (size_t)(entityCountLimit % edgeCountLeft != 0);
        const size_t edgeSpanCount = edgeCountLeft / entityCountLimit
                                   + edgeModulo;

        diskNetwork.setNameId(net->getName().getID());
        diskNetwork.setNodeCount(nodeCountLeft);
        diskNetwork.setEdgeCount(edgeCountLeft);
        auto nodeSpans = diskNetwork.initNodeSpans(nodeSpanCount);
        auto edgeSpans = diskNetwork.initEdgeSpans(edgeSpanCount);

        // Nodes
        size_t index = 0;
        for (auto nodeSpan : nodeSpans) {
            const size_t count = nodeCountLeft < entityCountLimit
                                   ? nodeCountLeft
                                   : entityCountLimit;

            auto diskNodes = nodeSpan.initNodes(count);

            for (size_t j = index; j < index + count; j++) {
                const size_t localj = j - index;
                const Node* node = nodes[j];
                auto props = node->properties();
                auto diskProperties = diskNodes[localj].initProperties(props.size());
                diskNodes[localj].setId(j);
                diskNodes[localj].setNameId(node->getName().getID());
                diskNodes[localj].setNodeTypeNameId(node->getType()->getName().getID());

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
                            Log::BioLog::log(msg::FATAL_INVALID_PROPERTY_DUMP());
                            return false;
                        }
                        case ValueType::_SIZE: {
                            Log::BioLog::log(msg::FATAL_INVALID_PROPERTY_DUMP());
                            return false;
                        }
                    }
                    k++;
                }
            }

            nodeCountLeft -= entityCountLimit;
            index += count;
        }

        // Edges
        index = 0;
        for (auto edgeSpan : edgeSpans) {
            const size_t count = edgeCountLeft < entityCountLimit
                                   ? edgeCountLeft
                                   : entityCountLimit;
            auto diskEdges = edgeSpan.initEdges(count);

            for (size_t j = index; j < index + count; j++) {
                const size_t localj = j - index;
                const Edge* edge = edges[j];
                auto props = edge->properties();
                auto diskProperties = diskEdges[localj].initProperties(props.size());
                const StringRef edgeTypeName = edge->getType()->getName();
                diskEdges[localj].setId(localj);
                diskEdges[localj].setEdgeTypeNameId(edgeTypeName.getID());
                diskEdges[localj].setSourceId(edge->getSource()->getIndex());
                diskEdges[localj].setTargetId(edge->getTarget()->getIndex());

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
                            Log::BioLog::log(msg::FATAL_INVALID_PROPERTY_DUMP());
                            return false;
                        }
                        case ValueType::_SIZE: {
                            Log::BioLog::log(msg::FATAL_INVALID_PROPERTY_DUMP());
                            return false;
                        }
                    }
                    k++;
                }
            }

            edgeCountLeft -= entityCountLimit;
            index += count;
        }

        i++;
    }

    ::capnp::writeMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}

}
