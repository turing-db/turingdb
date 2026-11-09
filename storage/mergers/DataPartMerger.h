#pragma once

#include <stddef.h>

#include <memory>

#include "ID.h"
#include "datapart/DataPartSpan.h"
#include "views/GraphView.h"

namespace db {
class MetadataBuilder;
class GraphView;
class DataPartBuilder;

class DataPartMerger {
public:
    DataPartMerger(const CommitData* commitData,
                   MetadataBuilder& metadataBuilder);

    std::unique_ptr<DataPartBuilder> merge(DataPartSpan dataParts) const;

private:
    GraphView _graphView;
    MetadataBuilder& _metadataBuilder;

    [[noreturn]] void throwEmbeddingDimensionError(PropertyTypeID propertyID,
                                                   size_t mergedDimension,
                                                   size_t partDimension) const;
};
}
