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

    DataPartRebaser(NodeID baseFstNodeID, EdgeID baseFstEdgeID, NodeID tgtFstNodeID,
                    EdgeID tgtFstEdgeID)
        : _baseCommitFirstNodeID(baseFstNodeID),
          _baseCommitFirstEdgeID(baseFstEdgeID),
          _targetCommitFirstNodeID(tgtFstNodeID),
          _targetCommitFirstEdgeID(tgtFstEdgeID)
    {
    }

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

    NodeID _baseCommitFirstNodeID {0};
    EdgeID _baseCommitFirstEdgeID {0};

    NodeID _targetCommitFirstNodeID {0};
    EdgeID _targetCommitFirstEdgeID {0};

    NodeID rebaseNodeID(const NodeID& id) const {
        return id >= _baseCommitFirstNodeID
                 ? id + _targetCommitFirstNodeID - _baseCommitFirstNodeID
                 : id;
    }

    EdgeID rebaseEdgeID(const EdgeID& id) const {
        return id >= _baseCommitFirstEdgeID
                 ? id + _targetCommitFirstEdgeID - _baseCommitFirstEdgeID
                 : id;
    }
};

}
