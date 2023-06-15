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
#include <iostream>
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
    DB::NodeTypeRange nodeTypes = _db->nodeTypes();
    ::capnp::List<OnDisk::NodeType>::Builder ntListBuilder =
        types.initNodeTypes(nodeTypes.size());

    for (const NodeType* nt : nodeTypes) {
        size_t i = nt->getIndex();
        OnDisk::NodeType::Builder onDisk = ntListBuilder[i];
        onDisk.setNameId(nt->getName().getSharedString()->getID());

        auto propTypes = nt->propertyTypes();

        ::capnp::List<OnDisk::PropertyType>::Builder propBuilder =
            onDisk.initPropertyTypes(propTypes.size());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        for (const PropertyType* propType : propTypes) {
            size_t j = propType->getIndex() - currentPropTypeId;

            propBuilder[j].setId(propType->getIndex());
            propBuilder[j].setNameId(propType->getName().getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propBuilder[j].setKind(kind);
        }
        currentPropTypeId += propTypes.size();
    }

    // Edge Types
    DB::EdgeTypeRange edgeTypes = _db->edgeTypes();
    ::capnp::List<OnDisk::EdgeType>::Builder etListBuilder =
        types.initEdgeTypes(edgeTypes.size());

    for (const EdgeType* et : edgeTypes) {
        size_t i = et->getIndex();
        OnDisk::EdgeType::Builder onDisk = etListBuilder[i];
        onDisk.setNameId(et->getName().getSharedString()->getID());

        // Sources
        const auto& sources = et->sourceTypes();
        ::capnp::List<uint64_t>::Builder sourcesBuilder =
            onDisk.initSources(sources.size());

        size_t j = 0;
        for (const auto& source : sources) {
            sourcesBuilder.set(j, source->getName().getID());
            j++;
        }

        // Targets
        const auto& targets = et->targetTypes();
        ::capnp::List<uint64_t>::Builder targetsBuilder =
            onDisk.initTargets(targets.size());

        j = 0;
        for (const auto& target : targets) {
            targetsBuilder.set(j, target->getName().getID());
            j++;
        }

        // PropertyTypes
        auto propTypes = et->propertyTypes();

        ::capnp::List<OnDisk::PropertyType>::Builder propBuilder =
            onDisk.initPropertyTypes(propTypes.size());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        for (const auto& propType : propTypes) {
            size_t j = propType->getIndex() - currentPropTypeId;

            propBuilder[j].setId(propType->getIndex());
            propBuilder[j].setNameId(
                propType->getName().getSharedString()->getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propBuilder[j].setKind(kind);
        }
        currentPropTypeId += propTypes.size();
    }

    ::capnp::writePackedMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}
}
