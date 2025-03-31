#pragma once

#include <memory>
#include <string>

#include "iterators/ScanNodePropertiesIterator.h"
#include "columns/ColumnIDs.h"
#include "EntityID.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

template <SupportedType T>
class ScanNodesByPropertyStep {
public:
    struct Tag {};

    ScanNodesByPropertyStep(ColumnIDs* nodes,
                            PropertyType propType,
                            ColumnVector<typename T::Primitive>* propValues);
    ScanNodesByPropertyStep(ScanNodesByPropertyStep&& other) = default;
    ~ScanNodesByPropertyStep();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanNodePropertiesChunkWriter<T>>(ctxt->getGraphView(), _propType._id);
        _it->setNodeIDs(_nodes);
        _it->setProperties(_propValues);
    }

    inline void reset() {
        _it->reset();
    }

    inline bool isFinished() const {
        return !_it->isValid();
    }

    inline void execute() {
        _it->fill(ChunkConfig::CHUNK_SIZE);
    }

    void describe(std::string& descr) const;

private:
    ColumnIDs* _nodes {nullptr};
    ColumnVector<typename T::Primitive>* _propValues {nullptr};
    const PropertyType _propType;
    std::unique_ptr<ScanNodePropertiesChunkWriter<T>> _it;
};

using ScanNodesByPropertyInt64Step = ScanNodesByPropertyStep<types::Int64>;
using ScanNodesByPropertyUInt64Step = ScanNodesByPropertyStep<types::UInt64>;
using ScanNodesByPropertyStringStep = ScanNodesByPropertyStep<types::String>;
using ScanNodesByPropertyDoubleStep = ScanNodesByPropertyStep<types::Double>;
using ScanNodesByPropertyBoolStep = ScanNodesByPropertyStep<types::Bool>;

}
