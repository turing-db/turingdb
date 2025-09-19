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

    DataPartRebaser(NodeID baseNxtNodeID, EdgeID baseNxtEdgeID, NodeID tgtNxtNodeID,
                    EdgeID tgtNxtEdgeID)
        : _baseCommitNextNodeID(baseNxtNodeID),
          _baseCommitNextEdgeID(baseNxtEdgeID),
          _targetCommitNextNodeID(tgtNxtNodeID),
          _targetCommitNextEdgeID(tgtNxtEdgeID)
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

    NodeID _baseCommitNextNodeID {0};
    EdgeID _baseCommitNextEdgeID {0};

    NodeID _targetCommitNextNodeID {0};
    EdgeID _targetCommitNextEdgeID {0};

    NodeID rebaseNodeID(const NodeID& id) const {
        return id >= _baseCommitNextNodeID
                 ? id + _targetCommitNextNodeID - _baseCommitNextNodeID
                 : id;
    }

    EdgeID rebaseEdgeID(const EdgeID& id) const {
        return id >= _baseCommitNextEdgeID
                 ? id + _targetCommitNextEdgeID - _baseCommitNextEdgeID
                 : id;
    }
};

}
