#include "TypeDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "EdgeType.h"
#include "MsgCommon.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "Writeback.h"
#include "capnp/TypeIndex.capnp.h"

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <unistd.h>

namespace db {

TypeDumper::TypeDumper(db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db) {
}

bool TypeDumper::dump() {
    // Remove types file if it already exists
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

    ::capnp::MallocMessageBuilder message;

    OnDisk::TypeIndex::Builder types = message.initRoot<OnDisk::TypeIndex>();

    // Node Types
    {
        DB::NodeTypeRange nodeTypes = _db->nodeTypes();
        ::capnp::List<OnDisk::NodeType>::Builder listBuilder =
            types.initNodeTypes(nodeTypes.size());

        size_t i = 0;
        for (const NodeType* nt : nodeTypes) {
            OnDisk::NodeType::Builder onDisk = listBuilder[i];
            onDisk.setNameId(nt->getName().getSharedString()->getID());

            auto propTypes = nt->propertyTypes();

            ::capnp::List<OnDisk::PropertyType>::Builder propBuilder =
                onDisk.initPropertyTypes(propTypes.size());

            size_t j = 0;
            for (const auto& propType : propTypes) {
                propBuilder[j].setNameId(
                    propType->getName().getSharedString()->getID());
                propBuilder[j].setKind(static_cast<OnDisk::ValueKind>(
                    propType->getValueType().getKind()));
                j++;
            }
            i++;
        }
    }

    // Edge Types
    {
        DB::EdgeTypeRange edgeTypes = _db->edgeTypes();
        ::capnp::List<OnDisk::EdgeType>::Builder listBuilder =
            types.initEdgeTypes(edgeTypes.size());

        size_t i = 0;
        for (const EdgeType* et : edgeTypes) {
            OnDisk::EdgeType::Builder onDisk = listBuilder[i];
            onDisk.setNameId(et->getName().getSharedString()->getID());

            auto propTypes = et->propertyTypes();

            ::capnp::List<OnDisk::PropertyType>::Builder propBuilder =
                onDisk.initPropertyTypes(propTypes.size());

            size_t j = 0;
            for (const auto& propType : propTypes) {
                propBuilder[j].setNameId(
                    propType->getName().getSharedString()->getID());
                propBuilder[j].setKind(static_cast<OnDisk::ValueKind>(
                    propType->getValueType().getKind()));
                j++;
            }
            i++;
        }
    }

    ::capnp::writePackedMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}
}
