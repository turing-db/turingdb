#pragma once

#include <expected>
#include <stddef.h>
#include <string_view>

#include "ID.h"
#include "dataframe/ColumnTag.h"
#include "metadata/PropertyType.h"

namespace db {

using WriteResult = std::expected<void, std::string>;
using WriteError = std::unexpected<std::string>;

class Column;

class WriteProcessorTypes {
public:
    struct PropertyConstraint;

    using PendingNodeIndex = size_t;
    using PropertyConstraints = std::vector<PropertyConstraint>;
    using Labels = std::vector<std::string_view>;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type {ValueType::Invalid};
        Column* _col {nullptr}; // Column where the values of this property may be found
    };

    struct PropertyUpdate {
        std::string_view _propName;
        ValueType _type {ValueType::Invalid};
        Column* _col {nullptr}; // Column where the updated value may be found
        PropertyTypeID _pid;
    };

    struct PendingNode {
        PendingNode(Labels&& labels, PropertyConstraints&& props, std::string_view name, ColumnTag tag)
            : _labels(std::move(labels)),
            _properties(std::move(props)),
            _name(name),
            _tag(tag)
        {
        }

        Labels _labels;
        PropertyConstraints _properties;
        std::string_view _name;
        ColumnTag _tag;
    };

    struct PendingEdge {
        PendingEdge(PropertyConstraints&& props,
                    std::string_view type,
                    std::string_view name,
                    std::string_view srcName,
                    std::string_view tgtName,
                    ColumnTag tag,
                    ColumnTag srcTag,
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

        PropertyConstraints _properties;
        std::string_view _edgeType;
        std::string_view _name;
        std::string_view _srcName;
        std::string_view _tgtName;
        ColumnTag _tag;
        ColumnTag _srcTag;
        ColumnTag _tgtTag;
    };

    struct NodeUpdate {
        PropertyUpdate _propUpdate;
        ColumnTag _tag;
    };

    struct EdgeUpdate {
        PropertyUpdate _propUpdate;
        ColumnTag _tag;
    };

};

}
