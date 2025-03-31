#pragma once

#include <memory>
#include <string>
#include <sstream>

#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"
#include "types/SupportedType.h"
#include "iterators/GetPropertiesIterator.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

template <typename PropertyChunkWriter, SupportedType T>
class GetFilteredPropertyStep {
public:
    struct Tag {};

    using ColumnValues = ColumnVector<typename T::Primitive>;

    GetFilteredPropertyStep(const ColumnIDs* entityIDs,
                            const PropertyType propertyType,
                            ColumnValues* propValues,
                            ColumnVector<size_t>* indices,
                            ColumnMask* projectedMask)
        : _entityIDs(entityIDs),
        _propertyType(propertyType),
        _propValues(propValues),
        _indices(indices),
        _projectedMask(projectedMask)
    {
    }

    GetFilteredPropertyStep(GetFilteredPropertyStep&& other) = default;

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<PropertyChunkWriter>(ctxt->getGraphView(),
                                                    _propertyType._id,
                                                    _entityIDs);
        _it->setOutput(_propValues);
        _it->setIndices(_indices);
    }

    inline void reset() {
        _it->reset();
    }

    inline bool isFinished() const { return !_it->isValid(); }

    void execute() {
        _projectedMask->resize(_entityIDs->size());
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
    ColumnVector<size_t>* _indices {nullptr};
    ColumnMask* _projectedMask {nullptr};
    std::unique_ptr<PropertyChunkWriter> _it;
};

template <SupportedType T>
using GetFilteredNodePropertyStep = GetFilteredPropertyStep<GetNodePropertiesChunkWriter<T>, T>;

template <SupportedType T>
using GetFilteredEdgePropertyStep = GetFilteredPropertyStep<GetEdgePropertiesChunkWriter<T>, T>;

using GetFilteredNodePropertyInt64Step = GetFilteredNodePropertyStep<types::Int64>;
using GetFilteredNodePropertyUInt64Step = GetFilteredNodePropertyStep<types::UInt64>;
using GetFilteredNodePropertyDoubleStep = GetFilteredNodePropertyStep<types::Double>;
using GetFilteredNodePropertyStringStep = GetFilteredNodePropertyStep<types::String>;
using GetFilteredNodePropertyBoolStep = GetFilteredNodePropertyStep<types::Bool>;

using GetFilteredEdgePropertyInt64Step = GetFilteredEdgePropertyStep<types::Int64>;
using GetFilteredEdgePropertyUInt64Step = GetFilteredEdgePropertyStep<types::UInt64>;
using GetFilteredEdgePropertyDoubleStep = GetFilteredEdgePropertyStep<types::Double>;
using GetFilteredEdgePropertyStringStep = GetFilteredEdgePropertyStep<types::String>;
using GetFilteredEdgePropertyBoolStep = GetFilteredEdgePropertyStep<types::Bool>;

}
