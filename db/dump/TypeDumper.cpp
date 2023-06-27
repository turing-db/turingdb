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

    for (const NodeType* nt : nodeTypes) {
        const size_t i = nt->getIndex();
        const auto propTypes = nt->propertyTypes();
        auto propBuilder = ntListBuilder[i].initPropertyTypes(propTypes.size());

        ntListBuilder[i].setNameId(nt->getName().getSharedString()->getID());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        for (const PropertyType* propType : propTypes) {
            const size_t j = propType->getIndex() - currentPropTypeId;

            propBuilder[j].setId(propType->getIndex());
            propBuilder[j].setNameId(propType->getName().getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propBuilder[j].setKind(kind);
        }

        currentPropTypeId += propTypes.size();
    }

    // Edge Types
    const DB::EdgeTypeRange edgeTypes = _db->edgeTypes();
    auto etListBuilder = typeIndexBuilder.initEdgeTypes(edgeTypes.size());

    for (const EdgeType* et : edgeTypes) {
        const size_t i = et->getIndex();
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

        auto propBuilder = etListBuilder[i].initPropertyTypes(propTypes.size());

        size_t currentPropTypeId = -1;
        for (const PropertyType* propType : propTypes) {
            if (currentPropTypeId > propType->getIndex()) {
                currentPropTypeId = propType->getIndex();
            }
        }

        for (const auto& propType : propTypes) {
            const size_t j = propType->getIndex() - currentPropTypeId;

            propBuilder[j].setId(propType->getIndex());
            propBuilder[j].setNameId(
                propType->getName().getSharedString()->getID());
            const auto kind = static_cast<OnDisk::ValueKind>(
                propType->getValueType().getKind());
            propBuilder[j].setKind(kind);
        }
        currentPropTypeId += propTypes.size();
    }

    ::capnp::writeMessageToFd(indexFD, message);
    close(indexFD);

    return true;
}
}
