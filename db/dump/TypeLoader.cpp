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

static_assert((uint8_t)ValueType::ValueKind::VK_INVALID ==
              (uint8_t)OnDisk::ValueKind::INVALID);
static_assert((uint8_t)ValueType::ValueKind::VK_INT ==
              (uint8_t)OnDisk::ValueKind::INT);
static_assert((uint8_t)ValueType::ValueKind::VK_UNSIGNED ==
              (uint8_t)OnDisk::ValueKind::UNSIGNED);
static_assert((uint8_t)ValueType::ValueKind::VK_BOOL ==
              (uint8_t)OnDisk::ValueKind::BOOL);
static_assert((uint8_t)ValueType::ValueKind::VK_DECIMAL ==
              (uint8_t)OnDisk::ValueKind::DECIMAL);
static_assert((uint8_t)ValueType::ValueKind::VK_STRING_REF ==
              (uint8_t)OnDisk::ValueKind::STRING_REF);
static_assert((uint8_t)ValueType::ValueKind::VK_STRING ==
              (uint8_t)OnDisk::ValueKind::STRING);
static_assert((uint8_t)ValueType::ValueKind::_SIZE ==
              (uint8_t)OnDisk::ValueKind::ENUM_SIZE);

TypeLoader::TypeLoader(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db) {
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
    OnDisk::TypeIndex::Reader types = message.getRoot<OnDisk::TypeIndex>();
    const auto& diskNodeTypes = types.getNodeTypes();

    for (OnDisk::NodeType::Reader diskNodeType : diskNodeTypes) {
        db::NodeType* nt = wb.createNodeType(strIndex[diskNodeType.getNameId()]);

        for (const auto& diskPropType : diskNodeType.getPropertyTypes()) {
            const ValueType valType {
                static_cast<ValueType::ValueKind>(diskPropType.getKind())};

            const StringRef propName = strIndex[diskPropType.getNameId()];
            wb.addPropertyType(nt, propName, valType, diskPropType.getId());
        }
    }

    std::vector<NodeType*> sources;
    std::vector<NodeType*> targets;
    const auto& diskEdgeTypes = types.getEdgeTypes();

    for (OnDisk::EdgeType::Reader diskEdgeType : diskEdgeTypes) {
        sources.clear();
        targets.clear();

        for (const auto& diskSource : diskEdgeType.getSources()) {
            sources.push_back(_db->getNodeType(strIndex[diskSource]));
        }

        for (const auto& diskTarget : diskEdgeType.getTargets()) {
            targets.push_back(_db->getNodeType(strIndex[diskTarget]));
        }

        db::EdgeType* et = wb.createEdgeType(strIndex[diskEdgeType.getNameId()],
                                             sources, targets);

        for (const auto& diskPropType : diskEdgeType.getPropertyTypes()) {
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
