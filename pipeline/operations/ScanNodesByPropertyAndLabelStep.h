#pragma once

#include <memory>
#include <string>

#include "iterators/ScanNodePropertiesByLabelIterator.h"
#include "columns/ColumnIDs.h"
#include "EntityID.h"
#include "iterators/ChunkConfig.h"
#include "ExecutionContext.h"

namespace db {

template <SupportedType T>
class ScanNodesByPropertyAndLabel {
public:
    struct Tag {};

    ScanNodesByPropertyAndLabel(ColumnIDs* nodes,
                                PropertyType propType,
                                const LabelSetHandle& labelSet,
                                ColumnVector<typename T::Primitive>* propValues);
    ScanNodesByPropertyAndLabel(ScanNodesByPropertyAndLabel&& other) = default;
    ~ScanNodesByPropertyAndLabel();

    void prepare(ExecutionContext* ctxt) {
        _it = std::make_unique<ScanNodePropertiesByLabelChunkWriter<T>>(ctxt->getGraphView(), _propType._id, _labelSet);
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
    LabelSetHandle _labelSet;
    std::unique_ptr<ScanNodePropertiesByLabelChunkWriter<T>> _it;
};

using ScanNodesByPropertyAndLabelInt64Step = ScanNodesByPropertyAndLabel<types::Int64>;
using ScanNodesByPropertyAndLabelUInt64Step = ScanNodesByPropertyAndLabel<types::UInt64>;
using ScanNodesByPropertyAndLabelStringStep = ScanNodesByPropertyAndLabel<types::String>;
using ScanNodesByPropertyAndLabelDoubleStep = ScanNodesByPropertyAndLabel<types::Double>;
using ScanNodesByPropertyAndLabelBoolStep = ScanNodesByPropertyAndLabel<types::Bool>;

}
