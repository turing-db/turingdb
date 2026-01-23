#pragma once

#include <stddef.h>

#include "ID.h"

namespace db {

class DataPart;
class MetadataRebaser;
class EntityIDRebaser;

class DataPartRebaser {
public:
    DataPartRebaser() = default;
    ~DataPartRebaser() = default;

    DataPartRebaser(EntityIDRebaser* idRebaser)
        : _idRebaser(idRebaser)
    {
    }

    DataPartRebaser(const DataPartRebaser&) = default;
    DataPartRebaser& operator=(const DataPartRebaser&) = default;
    DataPartRebaser(DataPartRebaser&&) = default;
    DataPartRebaser& operator=(DataPartRebaser&&) = default;

    bool rebase(const MetadataRebaser& metadata,
                const DataPart& prevPart,
                DataPart& part);
private:
    EntityIDRebaser* _idRebaser {nullptr};

    NodeID _prevFirstNodeID {0};
    EdgeID _prevFirstEdgeID {0};

    size_t _nodeOffset {0};
    size_t _edgeOffset {0};
};

}
