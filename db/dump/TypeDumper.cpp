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
#include <capnp/serialize.h>
#include <unistd.h>

namespace db {

TypeDumper::TypeDumper(const db::DB* db, const FileUtils::Path& indexPath)
    : _indexPath(indexPath),
      _db(db)
{
}

bool TypeDumper::dump() {
    // Remove types file if it already exists
    if (FileUtils::exists(_indexPath)) {
        if (!FileUtils::removeFile(_indexPath)) {
            Log::BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE() << _indexPath);
            return false;
        }
    }

    const int indexFD = FileUtils::openForWrite(_indexPath);
    if (indexFD < 0) {
        return false;
    }

    ::capnp::MallocMessageBuilder message;
    auto typeIndexBuilder = message.initRoot<OnDisk::TypeIndex>();

    // Node Types
    const DB::NodeTypeRange nodeTypes = _db->nodeTypes();
    auto ntListBuilder = typeIndexBuilder.initNodeTypes(nodeTypes.size());

    size_t i = 0;
    for (const NodeType* nt : nodeTypes) {
        auto ntBuilder = ntListBuilder[i];
        ntBuilder.setNameId((uint64_t)nt->getName().getSharedString()->getID());
        const auto propTypes = nt->propertyTypes();
        auto propTypeListBuilder = ntBuilder.initPropertyTypes(propTypes.size());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        size_t j = 0;
        for (const PropertyType* propType : propTypes) {
            auto propTypeBuilder = propTypeListBuilder[j];

            propTypeBuilder.setId(propType->getIndex());
            propTypeBuilder.setNameId((uint64_t)propType->getName().getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propTypeBuilder.setKind(kind);
            j++;
        }

        currentPropTypeId += propTypes.size();
        i++;
    }

    // Edge Types
    const DB::EdgeTypeRange edgeTypes = _db->edgeTypes();
    auto etListBuilder = typeIndexBuilder.initEdgeTypes(edgeTypes.size());

    i = 0;
    for (const EdgeType* et : edgeTypes) {
        etListBuilder[i].setNameId(et->getName().getSharedString()->getID());

        // Sources
        const auto& sources = et->sourceTypes();
        auto sourcesBuilder = etListBuilder[i].initSources(sources.size());

        size_t j = 0;
        for (const auto& source : sources) {
            sourcesBuilder.set(j, source->getName().getID());
            j++;
        }

        // Targets
        const auto& targets = et->targetTypes();
        auto targetsBuilder = etListBuilder[i].initTargets(targets.size());

        j = 0;
        for (const auto& target : targets) {
            targetsBuilder.set(j, target->getName().getID());
            j++;
        }

        // PropertyTypes
        auto propTypes = et->propertyTypes();
        auto propTypeListBuilder = etListBuilder[i].initPropertyTypes(propTypes.size());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        size_t k = 0;
        for (const auto& propType : propTypes) {
            auto propTypeBuilder = propTypeListBuilder[k];
            propTypeBuilder.setId(propType->getIndex());
            propTypeBuilder.setNameId(
                propType->getName().getSharedString()->getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propTypeBuilder.setKind(kind);
            k++;
        }
        currentPropTypeId += propTypes.size();
        i++;
    }

    ::capnp::writeMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}
}
