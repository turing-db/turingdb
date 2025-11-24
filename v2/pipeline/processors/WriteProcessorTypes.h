#pragma once

#include <cstddef>
#include <string_view>
#include <variant>

#include "dataframe/ColumnTag.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class WriteProcessorTypes {
public:
    using PendingNodeOffset = size_t;
    // TODO: Check this is what it should be
    using IncidentNode = std::variant<PendingNodeOffset, ColumnTag>;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type;
        ColumnTag _tag; // Column where the values of this property may be found
    };

    struct PendingNode {
        std::vector<std::string_view> _labels;
        std::vector<PropertyConstraint> _properties;
    };

    struct PendingEdge {
        std::vector<PropertyConstraint> _properties;
        std::string_view _edgeType;
        IncidentNode _srcTag;
        IncidentNode _tgtTag;
    };

    struct NodeUpdate {
        PropertyConstraint* _propUpdate;
        ColumnTag _tag;
    };

    struct EdgeUpdate {
        PropertyConstraint* _propUpdate;
        ColumnTag _tag;
    };

};

}
