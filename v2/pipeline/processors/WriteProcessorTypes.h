#pragma once

#include <cstddef>
#include <string_view>

#include "dataframe/ColumnTag.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class WriteProcessorTypes {
public:
    struct PropertyConstraint;

    using PendingNodeIndex = size_t;
    // TODO: Check this is what it should be
    using PropertyConstraints = std::vector<PropertyConstraint>;
    using Labels = std::vector<std::string_view>;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type;
        ColumnTag _tag; // Column where the values of this property may be found
    };

    struct PendingNode {
        Labels _labels;
        PropertyConstraints _properties;
        std::string_view _name;
        ColumnTag _tag;

        PendingNode(Labels&& labels, PropertyConstraints&& props, std::string_view name, ColumnTag tag)
            : _labels(std::move(labels)),
            _properties(std::move(props)),
            _name(name),
            _tag(tag)
        {
        }
    };

    struct PendingEdge {
        PropertyConstraints _properties;
        std::string_view _edgeType;
        std::string_view _name;
        std::string_view _srcName;
        std::string_view _tgtName;
        ColumnTag _tag;
        ColumnTag _srcTag;
        ColumnTag _tgtTag;

        PendingEdge(PropertyConstraints&& props, std::string_view type,
                    std::string_view name, std::string_view srcName,
                    std::string_view tgtName, ColumnTag tag, ColumnTag srcTag,
                    ColumnTag tgtTag)
            : _properties(std::move(props)),
              _edgeType(type),
              _name(name),
              _srcName(srcName),
              _tgtName(tgtName),
              _tag(tag),
              _srcTag(srcTag),
              _tgtTag(tgtTag)
        {
        }
    };

    struct NodeUpdate {
        PropertyConstraint* _propUpdate {nullptr};
        ColumnTag _tag;
    };

    struct EdgeUpdate {
        PropertyConstraint* _propUpdate {nullptr};
        ColumnTag _tag;
    };

};

}
