#pragma once

#include <cstddef>

#include "ID.h"

namespace db {

class DataPart;
class MetadataRebaser;

class DataPartRebaser {
public:
    DataPartRebaser() = default;
    ~DataPartRebaser() = default;

    DataPartRebaser(const DataPartRebaser&) = delete;
    DataPartRebaser(DataPartRebaser&&) = delete;
    DataPartRebaser& operator=(const DataPartRebaser&) = delete;
    DataPartRebaser& operator=(DataPartRebaser&&) = delete;

    bool rebase(const MetadataRebaser& metadata,
                const DataPart& prevPart,
                DataPart& part);
private:
    NodeID _prevFirstNodeID {0};
    EdgeID _prevFirstEdgeID {0};
    size_t _nodeOffset {0};
    size_t _edgeOffset {0};

    NodeID rebaseNodeID(const NodeID& id) const {
        return id >= _prevFirstNodeID ? id + _nodeOffset : id;
    }

    EdgeID rebaseEdgeID(const EdgeID& id) const {
        return id >= _prevFirstEdgeID ? id + _edgeOffset : id;
    }
};

}
