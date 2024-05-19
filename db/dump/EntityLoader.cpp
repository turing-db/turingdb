#include "EntityLoader.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

#include "BioAssert.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "StringIndex.h"
#include "StringIndexLoader.h"
#include "Writeback.h"
#include "capnp/EntityIndex.capnp.h"
#include "LogUtils.h"

namespace db {

EntityLoader::EntityLoader(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db)
{
}

bool EntityLoader::load(const StringIndexLoader& strLoader) {
    if (!FileUtils::exists(_indexPath)) {
        logt::FileNotFound(_indexPath.string());
        return false;
    }

    const int indexFD = FileUtils::openForRead(_indexPath);
    if (indexFD < 0) {
        logt::CanNotRead(_indexPath.string());
        return false;
    }

    Writeback wb(_db);

    ::capnp::StreamFdMessageReader message(indexFD, {
        .traversalLimitInWords = 100000000000000,
        .nestingLimit = 128,
    });

    const auto entityIndexReader = message.getRoot<OnDisk::EntityIndex>();

    for (auto diskNetwork : entityIndexReader.getNetworks()) {
        Network* net = wb.createNetwork(strLoader[diskNetwork.getNameId()]);
        bioassert(net);
    }

    // Nodes
    for (const auto nodeSpan : entityIndexReader.getNodeSpans()) {
        for (const auto diskNode : nodeSpan.getNodes()) {
            const StringRef ntName = strLoader[diskNode.getNodeTypeNameId()];
            const StringRef nName = strLoader[diskNode.getNameId()];
            Network* net = _db->getNetwork(strLoader[diskNode.getNetworkId()]);
            NodeType* nt = _db->getNodeType(ntName);
            Node* n = wb.createNode(net, nt, nName);
            bioassert(n);
            const auto& props = diskNode.getProperties();

            for (const auto diskProp : props) {
                const ValueType valType {
                    static_cast<ValueType::ValueKind>(diskProp.getKind())};
                const StringRef propName =
                    strLoader[diskProp.getPropertyTypeNameId()];
                const PropertyType* propType = nt->getPropertyType(propName);

                switch (valType.getKind()) {
                    case (ValueType::VK_INT): {
                        const int64_t v = diskProp.getValue().getInt();
                        wb.setProperty(n, {propType, Value::createInt(v)});
                        break;
                    }
                    case (ValueType::VK_UNSIGNED): {
                        const uint64_t v = diskProp.getValue().getUnsigned();
                        wb.setProperty(n, {propType, Value::createUnsigned(v)});
                        break;
                    }
                    case (ValueType::VK_BOOL): {
                        const bool v = diskProp.getValue().getBool();
                        wb.setProperty(n, {propType, Value::createBool(v)});
                        break;
                    }
                    case (ValueType::VK_DECIMAL): {
                        const double v = diskProp.getValue().getDecimal();
                        wb.setProperty(n, {propType, Value::createDouble(v)});
                        break;
                    }
                    case (ValueType::VK_STRING_REF): {
                        const size_t v = diskProp.getValue().getStringRefId();
                        wb.setProperty(
                            n, {propType, Value::createStringRef(strLoader[v])});
                        break;
                    }
                    case (ValueType::VK_STRING): {
                        wb.setProperty(
                            n,
                            {propType, Value::createString(diskProp.getValue().getString())});
                        break;
                    }
                    case (ValueType::VK_INVALID): {
                        spdlog::error("Load of invalid property");
                        return false;
                    }
                    case (ValueType::_SIZE): {
                        spdlog::error("Load of property of invalid size");
                        return false;
                    }
                }
            }
        }
    }

    // Edges
    for (const auto edgeSpan : entityIndexReader.getEdgeSpans()) {
        for (const auto diskEdge : edgeSpan.getEdges()) {
            const StringRef etName = strLoader[diskEdge.getEdgeTypeNameId()];
            EdgeType* et = _db->getEdgeType(etName);
            const size_t sourceId = diskEdge.getSourceId();
            const size_t targetId = diskEdge.getTargetId();
            Node* sourceNode = _db->getNode((DBIndex)sourceId);

            msgbioassert(sourceNode,
                std::string(
                    "Node map of size " + std::to_string(_db->getNodeCount())
                    + " does not have the required index: " + std::to_string(sourceId)).c_str());
            Node* targetNode = _db->getNode((DBIndex)targetId);

            msgbioassert(targetNode,
                std::string(
                    "Node map of size " + std::to_string(_db->getNodeCount())
                    + " does not have the required index: " + std::to_string(targetId)).c_str());

            Edge* e = wb.createEdge(et, sourceNode, targetNode);

            const auto& props = diskEdge.getProperties();

            for (const auto diskProp : props) {
                const auto diskKind = diskProp.getKind();
                const ValueType valType {
                    static_cast<ValueType::ValueKind>(diskKind)};
                const StringRef propName =
                    strLoader[diskProp.getPropertyTypeNameId()];
                const PropertyType* propType = et->getPropertyType(propName);

                switch (valType.getKind()) {
                    case (ValueType::VK_INT): {
                        const int64_t v = diskProp.getValue().getInt();
                        wb.setProperty(e, {propType, Value::createInt(v)});
                        break;
                    }
                    case (ValueType::VK_UNSIGNED): {
                        const uint64_t v = diskProp.getValue().getUnsigned();
                        wb.setProperty(e, {propType, Value::createUnsigned(v)});
                        break;
                    }
                    case (ValueType::VK_BOOL): {
                        const bool v = diskProp.getValue().getBool();
                        wb.setProperty(e, {propType, Value::createBool(v)});
                        break;
                    }
                    case (ValueType::VK_DECIMAL): {
                        const double v = diskProp.getValue().getDecimal();
                        wb.setProperty(e, {propType, Value::createDouble(v)});
                        break;
                    }
                    case (ValueType::VK_STRING_REF): {
                        const size_t v = diskProp.getValue().getStringRefId();
                        wb.setProperty(
                            e, {propType, Value::createStringRef(strLoader[v])});
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
                        spdlog::error("Load of invalid property");
                        return false;
                    }
                    case (ValueType::_SIZE): {
                        spdlog::error("Load of property of invalid size");
                        return false;
                    }
                }
            }
        }
    }

    close(indexFD);
    return true;
}
}
