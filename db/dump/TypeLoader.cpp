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

    const int entityIndexFD = FileUtils::openForRead(_indexPath);

    if (entityIndexFD < 0) {
        return false;
    }

    Writeback wb(_db);

    ::capnp::PackedFdMessageReader message(entityIndexFD);
    OnDisk::TypeIndex::Reader types = message.getRoot<OnDisk::TypeIndex>();

    for (OnDisk::NodeType::Reader node : types.getNodeTypes()) {
        db::NodeType* nt = wb.createNodeType(strIndex[node.getNameId()]);

        for (const auto& diskPropType : node.getPropertyTypes()) {
            const ValueType valType {
                static_cast<ValueType::ValueKind>(diskPropType.getKind())};

            const StringRef propName = strIndex[diskPropType.getNameId()];
            wb.addPropertyType(nt, propName, valType);
        }
    }

    std::vector<NodeType*> sources;
    std::vector<NodeType*> targets;

    for (OnDisk::EdgeType::Reader diskEdgeType : types.getEdgeTypes()) {
        sources.clear();
        targets.clear();

        for (const auto& diskSource : diskEdgeType.getSources()) {
            sources.push_back(_db->getNodeType(strIndex[diskSource]));
        }

        for (const auto& diskTarget : diskEdgeType.getSources()) {
            targets.push_back(_db->getNodeType(strIndex[diskTarget]));
        }

        db::EdgeType* et = wb.createEdgeType(strIndex[diskEdgeType.getNameId()],
                                             sources, targets);

        for (const auto& diskPropType : diskEdgeType.getPropertyTypes()) {
            const ValueType valType {
                static_cast<ValueType::ValueKind>(diskPropType.getKind())};
            const StringRef propName = strIndex[diskPropType.getNameId()];
            wb.addPropertyType(et, propName, valType);
        }
    }

    close(entityIndexFD);
    return true;
}
}
