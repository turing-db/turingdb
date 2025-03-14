#pragma once

#include <memory>
#include <string>
#include <sstream>

#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "types/SupportedType.h"
#include "iterators/GetPropertiesIterator.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

template <typename PropertyChunkWriter, SupportedType T> 
class GetPropertyStep {
public:
    struct Tag {};

    using ColumnValues = ColumnOptVector<typename T::Primitive>;

    GetPropertyStep(const ColumnIDs* entityIDs,
                    PropertyType propertyType,
                    ColumnValues* propValues)
        : _entityIDs(entityIDs)
        , _propertyType(propertyType)
        , _propValues(propValues)
    {
    }

    GetPropertyStep(GetPropertyStep&& other) = default;

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(),
                                                    _propertyType._id,
                                                    _entityIDs);
        _it->setOutput(_propValues);
    }

    inline void reset() {
        _it->reset();
    }

    inline bool isFinished() const { return !_it->isValid(); }

    void execute() {
        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

    void describe(std::string& descr) const {
        std::stringstream ss;
        ss << "GetPropertyStep";
        ss << " entityIDs=" << std::hex << _entityIDs;
        ss << " propertyType=" << _propertyType._id;
        ss << " propValues=" << std::hex << _propValues;
        descr = ss.str();
    }

private:
    const ColumnIDs* _entityIDs {nullptr};
    PropertyType _propertyType;
    ColumnValues* _propValues {nullptr};
    std::unique_ptr<PropertyChunkWriter> _it;
};

template <SupportedType T>
using GetNodePropertyStep = GetPropertyStep<GetNodePropertiesWithNullChunkWriter<T>, T>;

template <SupportedType T>
using GetEdgePropertyStep = GetPropertyStep<GetEdgePropertiesWithNullChunkWriter<T>, T>;

using GetNodePropertyInt64Step = GetNodePropertyStep<types::Int64>;
using GetNodePropertyUInt64Step = GetNodePropertyStep<types::UInt64>;
using GetNodePropertyDoubleStep = GetNodePropertyStep<types::Double>;
using GetNodePropertyStringStep = GetNodePropertyStep<types::String>;
using GetNodePropertyBoolStep = GetNodePropertyStep<types::Bool>;

using GetEdgePropertyInt64Step = GetEdgePropertyStep<types::Int64>;
using GetEdgePropertyUInt64Step = GetEdgePropertyStep<types::UInt64>;
using GetEdgePropertyDoubleStep = GetEdgePropertyStep<types::Double>;
using GetEdgePropertyStringStep = GetEdgePropertyStep<types::String>;
using GetEdgePropertyBoolStep = GetEdgePropertyStep<types::Bool>;

} 
