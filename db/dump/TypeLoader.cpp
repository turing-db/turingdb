#include "TypeLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "EdgeType.h"
#include "NodeType.h"
#include "Writeback.h"
#include "capnp/TypeIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <unistd.h>

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

bool TypeLoader::load(const std::vector<StringRef>& strIndex) {
    if (!FileUtils::exists(_indexPath)) {
        return false;
    }

    const int indexFD = FileUtils::openForRead(_indexPath);

    if (indexFD < 0) {
        return false;
    }

    Writeback wb(_db);

    ::capnp::PackedFdMessageReader message(indexFD);
    const auto typeIndexReader = message.getRoot<OnDisk::TypeIndex>();

    for (const auto diskNodeType : typeIndexReader.getNodeTypes()) {
        db::NodeType* nt = wb.createNodeType(strIndex[diskNodeType.getNameId()]);

        for (const auto diskPropType : diskNodeType.getPropertyTypes()) {
            const ValueType valType {
                static_cast<ValueType::ValueKind>(diskPropType.getKind())};

            const StringRef propName = strIndex[diskPropType.getNameId()];
            wb.addPropertyType(nt, propName, valType, diskPropType.getId());
        }
    }

    std::vector<NodeType*> sources;
    std::vector<NodeType*> targets;

    for (const auto diskEdgeType : typeIndexReader.getEdgeTypes()) {
        sources.clear();
        targets.clear();

        for (const auto diskSource : diskEdgeType.getSources()) {
            sources.push_back(_db->getNodeType(strIndex[diskSource]));
        }

        for (const auto diskTarget : diskEdgeType.getTargets()) {
            targets.push_back(_db->getNodeType(strIndex[diskTarget]));
        }

        db::EdgeType* et = wb.createEdgeType(strIndex[diskEdgeType.getNameId()],
                                             sources, targets);

        for (const auto diskPropType : diskEdgeType.getPropertyTypes()) {
            const auto kind = static_cast<ValueType::ValueKind>(diskPropType.getKind());
            const ValueType valType {kind};
            const StringRef propName = strIndex[diskPropType.getNameId()];

            wb.addPropertyType(et, propName, valType, diskPropType.getId());
        }
    }

    close(indexFD);
    return true;
}
}
