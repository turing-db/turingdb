#pragma once

#include <memory>

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
};
}
