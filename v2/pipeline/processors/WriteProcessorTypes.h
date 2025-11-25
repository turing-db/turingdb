#pragma once

#include <cstddef>
#include <string_view>
#include <variant>

#include "dataframe/ColumnTag.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class WriteProcessorTypes {
public:
    struct PropertyConstraint;

    using PendingNodeIndex = size_t;
    // TODO: Check this is what it should be
    using IncidentNode = std::variant<PendingNodeIndex, ColumnTag>;
    using PropertyConstraints = std::vector<PropertyConstraint>;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type;
        ColumnTag _tag; // Column where the values of this property may be found
    };

    struct PendingNode {
        std::vector<std::string_view> _labels;
        std::vector<PropertyConstraint> _properties;
        std::string_view _name;
        ColumnTag _tag;
    };

    struct PendingEdge {
        std::vector<PropertyConstraint> _properties;
        std::string_view _edgeType;
        std::string_view _name;
        IncidentNode _srcTag;
        IncidentNode _tgtTag;
        ColumnTag _tag;
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
