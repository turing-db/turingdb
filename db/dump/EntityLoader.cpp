#include "EntityLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "StringIndex.h"
#include "Writeback.h"
#include "capnp/EntityIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <unistd.h>

namespace db {

EntityLoader::EntityLoader(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db) {
}

bool EntityLoader::load(const std::vector<StringRef>& strIndex) {
    if (!FileUtils::exists(_indexPath)) {
        return false;
    }

    int indexFD = FileUtils::openForRead(_indexPath);

    if (indexFD < 0) {
        return false;
    }

    Writeback wb(_db);

    ::capnp::PackedFdMessageReader message(indexFD);
    OnDisk::EntityIndex::Reader entities =
        message.getRoot<OnDisk::EntityIndex>();

    for (OnDisk::Network::Reader diskNetwork : entities.getNetworks()) {
        Network* net = wb.createNetwork(strIndex[diskNetwork.getNameId()]);

        // Nodes
        for (const auto& diskNode : diskNetwork.getNodes()) {
            const StringRef ntName = strIndex[diskNode.getNodeTypeNameId()];
            const StringRef nName = strIndex[diskNode.getNameId()];
            NodeType* nt = _db->getNodeType(ntName);
            Node* n = wb.createNode(net, nt, nName);

            for (const auto& diskProp : diskNode.getProperties()) {
                const ValueType valType {
                    static_cast<ValueType::ValueKind>(diskProp.getKind())};
                const StringRef propName =
                    strIndex[diskProp.getPropertyTypeNameId()];
                const PropertyType* propType = nt->getPropertyType(propName);

                switch (valType.getKind()) {
                    case (ValueType::VK_INT): {
                        int64_t v = diskProp.getValue().getInt();
                        wb.setProperty(n, {propType, Value::createInt(v)});
                        break;
                    }
                    case (ValueType::VK_UNSIGNED): {
                        uint64_t v = diskProp.getValue().getUnsigned();
                        wb.setProperty(n, {propType, Value::createUnsigned(v)});
                        break;
                    }
                    case (ValueType::VK_BOOL): {
                        bool v = diskProp.getValue().getBool();
                        wb.setProperty(n, {propType, Value::createBool(v)});
                        break;
                    }
                    case (ValueType::VK_DECIMAL): {
                        double v = diskProp.getValue().getDecimal();
                        wb.setProperty(n, {propType, Value::createDouble(v)});
                        break;
                    }
                    case (ValueType::VK_STRING_REF): {
                        size_t v = diskProp.getValue().getStringRefId();
                        wb.setProperty(
                            n, {propType, Value::createStringRef(strIndex[v])});
                        break;
                    }
                    case (ValueType::VK_STRING): {
                        wb.setProperty(
                            n,
                            {propType, Value::createString(
                                           diskProp.getValue().getString())});
                        break;
                    }
                    case (ValueType::VK_INVALID): {
                        Log::BioLog::echo("[FATAL ERROR, SHOULD NOT OCCUR] An "
                                          "invalid property "
                                          "type was loaded from a dumped db");
                        break;
                    }
                    case (ValueType::_SIZE): {
                        Log::BioLog::echo("[FATAL ERROR, SHOULD NOT OCCUR] An "
                                          "invalid property "
                                          "type was loaded from a dumped db");
                        break;
                    }
                }
            }
        }

        // Edges
        for (const auto& diskEdge : diskNetwork.getEdges()) {
            StringRef etName = strIndex[diskEdge.getEdgeTypeNameId()];
            EdgeType* et = _db->getEdgeType(etName);
            size_t sourceId = diskEdge.getSourceId();
            size_t targetId = diskEdge.getTargetId();
            Node* sourceNode = net->getNode((DBIndex)sourceId);
            Node* targetNode = net->getNode((DBIndex)targetId);

            Edge* e = wb.createEdge(et, sourceNode, targetNode);

            for (const auto& diskProp : diskEdge.getProperties()) {
                const ValueType valType {
                    (ValueType::ValueKind)diskProp.getKind()};
                const StringRef propName =
                    strIndex[diskProp.getPropertyTypeNameId()];
                const PropertyType* propType = et->getPropertyType(propName);

                switch (valType.getKind()) {
                    case (ValueType::VK_INT): {
                        int64_t v = diskProp.getValue().getInt();
                        wb.setProperty(e, {propType, Value::createInt(v)});
                        break;
                    }
                    case (ValueType::VK_UNSIGNED): {
                        uint64_t v = diskProp.getValue().getUnsigned();
                        wb.setProperty(e, {propType, Value::createUnsigned(v)});
                        break;
                    }
                    case (ValueType::VK_BOOL): {
                        bool v = diskProp.getValue().getBool();
                        wb.setProperty(e, {propType, Value::createBool(v)});
                        break;
                    }
                    case (ValueType::VK_DECIMAL): {
                        double v = diskProp.getValue().getDecimal();
                        wb.setProperty(e, {propType, Value::createDouble(v)});
                        break;
                    }
                    case (ValueType::VK_STRING_REF): {
                        size_t v = diskProp.getValue().getStringRefId();
                        wb.setProperty(
                            e, {propType, Value::createStringRef(strIndex[v])});
                        break;
                    }
                    case (ValueType::VK_STRING): {
                        wb.setProperty(
                            e,
                            {propType, Value::createString(
                                           diskProp.getValue().getString())});
                        break;
                    }
                    case (ValueType::VK_INVALID): {
                        Log::BioLog::echo("[FATAL ERROR, SHOULD NOT OCCUR] An "
                                          "invalid property "
                                          "type was loaded from a dumped db");
                        break;
                    }
                    case (ValueType::_SIZE): {
                        Log::BioLog::echo("[FATAL ERROR, SHOULD NOT OCCUR] An "
                                          "invalid property "
                                          "type was loaded from a dumped db");
                        break;
                    }
                }
            }
        }
    }

    close(indexFD);
    return true;
}
}
