#pragma once

#include <array>

#include "dataframe/ColumnTag.h"

namespace db::v2 {

class EntityOutputStream {
public:
    EntityOutputStream() = default;

    static EntityOutputStream createEmptyStream() {
        return EntityOutputStream {};
    }

    static EntityOutputStream createNodeStream(ColumnTag nodeIDsTag) {
        EntityOutputStream stream;
        stream._tags[0] = nodeIDsTag;
        return stream;
    }

    static EntityOutputStream createEdgeStream(ColumnTag edgeIDsTag,
                                               ColumnTag otherIDsTag,
                                               ColumnTag edgeTypesTag) {
        EntityOutputStream stream;
        stream._tags[1] = edgeIDsTag;
        stream._tags[2] = otherIDsTag;
        stream._tags[3] = edgeTypesTag;
        return stream;
    }

    ColumnTag getNodeIDsTag() const {
        return _tags[0];
    }

    ColumnTag getEdgeIDsTag() const {
        return _tags[1];
    }

    ColumnTag getOtherIDsTag() const {
        return _tags[2];
    }

    ColumnTag getEdgeTypesTag() const {
        return _tags[3];
    }

private:
    std::array<ColumnTag, 4> _tags;
};

}
