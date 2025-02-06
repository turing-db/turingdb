#pragma once

#include <memory>

#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "types/SupportedType.h"
#include "iterators/GetNodePropertiesIterator.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

template <SupportedType T> 
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
        _it = std::make_unique<GetNodePropertiesWithNullChunkWriter<T>>(ctxt->getGraphView(),
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

private:
    const ColumnIDs* _entityIDs;
    PropertyType _propertyType;
    ColumnValues* _propValues;
    std::unique_ptr<GetNodePropertiesWithNullChunkWriter<T>> _it;
};

using GetPropertyInt64Step = GetPropertyStep<types::Int64>;
using GetPropertyUInt64Step = GetPropertyStep<types::UInt64>;
using GetPropertyDoubleStep = GetPropertyStep<types::Double>;
using GetPropertyStringStep = GetPropertyStep<types::String>;
using GetPropertyBoolStep = GetPropertyStep<types::Bool>;

} 
