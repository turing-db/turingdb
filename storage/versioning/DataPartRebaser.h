#pragma once

#include <cstddef>

#include "ID.h"

namespace db {

class DataPart;
class MetadataRebaser;
class ChangeRebaser;

class DataPartRebaser {
public:
    DataPartRebaser() = default;
    ~DataPartRebaser() = default;

    DataPartRebaser(ChangeRebaser* changeRebaser)
        : _changeRebaser(changeRebaser)
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
    ChangeRebaser* _changeRebaser {nullptr};

    NodeID _prevFirstNodeID {0};
    EdgeID _prevFirstEdgeID {0};

    size_t _nodeOffset {0};
    size_t _edgeOffset {0};
};

}
