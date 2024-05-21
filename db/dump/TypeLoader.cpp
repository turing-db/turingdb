#include "TypeLoader.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <unistd.h>

#include "DB.h"
#include "EdgeType.h"
#include "NodeType.h"
#include "StringIndexLoader.h"
#include "Writeback.h"
#include "capnp/TypeIndex.capnp.h"
#include "LogUtils.cpp"

namespace db {

static_assert((uint8_t)ValueType::VK_INVALID == (uint8_t)OnDisk::ValueKind::INVALID);
static_assert((uint8_t)ValueType::VK_INT == (uint8_t)OnDisk::ValueKind::INT);
static_assert((uint8_t)ValueType::VK_UNSIGNED == (uint8_t)OnDisk::ValueKind::UNSIGNED);
static_assert((uint8_t)ValueType::VK_BOOL == (uint8_t)OnDisk::ValueKind::BOOL);
static_assert((uint8_t)ValueType::VK_DECIMAL == (uint8_t)OnDisk::ValueKind::DECIMAL);
static_assert((uint8_t)ValueType::VK_STRING_REF == (uint8_t)OnDisk::ValueKind::STRING_REF);
static_assert((uint8_t)ValueType::VK_STRING == (uint8_t)OnDisk::ValueKind::STRING);
static_assert((uint8_t)ValueType::_SIZE == (uint8_t)OnDisk::ValueKind::ENUM_SIZE);

TypeLoader::TypeLoader(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db)
{
}

bool TypeLoader::load(const StringIndexLoader& strLoader) {
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
    const auto typeIndexReader = message.getRoot<OnDisk::TypeIndex>();

    for (const auto diskNodeType : typeIndexReader.getNodeTypes()) {
        const StringRef ntName = strLoader[diskNodeType.getNameId()];
        db::NodeType* nt = wb.createNodeType(ntName);

        for (const auto diskPropType : diskNodeType.getPropertyTypes()) {
            const ValueType valType {
                static_cast<ValueType::ValueKind>(diskPropType.getKind())};

            const StringRef propName = strLoader[diskPropType.getNameId()];
            wb.addPropertyType(nt, propName, valType, diskPropType.getId());
        }
    }

    std::vector<NodeType*> sources;
    std::vector<NodeType*> targets;

    for (const auto diskEdgeType : typeIndexReader.getEdgeTypes()) {
        sources.clear();
        targets.clear();

        for (const auto diskSource : diskEdgeType.getSources()) {
            const StringRef sourceName = strLoader[diskSource];
            sources.push_back(_db->getNodeType(sourceName));
        }

        for (const auto diskTarget : diskEdgeType.getTargets()) {
            const StringRef targetName = strLoader[diskTarget];
            targets.push_back(_db->getNodeType(targetName));
        }

        const StringRef edgeTypeName = strLoader[diskEdgeType.getNameId()];
        db::EdgeType* et = wb.createEdgeType(edgeTypeName, sources, targets);

        for (const auto diskPropType : diskEdgeType.getPropertyTypes()) {
            const auto kind = static_cast<ValueType::ValueKind>(diskPropType.getKind());
            const ValueType valType {kind};
            const StringRef propName = strLoader[diskPropType.getNameId()];

            wb.addPropertyType(et, propName, valType, diskPropType.getId());
        }
    }

    close(indexFD);
    return true;
}
}
