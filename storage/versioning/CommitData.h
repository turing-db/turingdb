#pragma once

#include <memory>
#include <vector>

#include "DataPartSpan.h"
#include "versioning/CommitHash.h"

namespace db {

class DataPart;
class CommitBuilder;
class GraphMetadata;
class VersionController;

class CommitData {
public:
    CommitHash hash() const { return _hash; }
    DataPartSpan dataparts() const { return _dataparts; }
    GraphMetadata& metadata() const { return *_graphMetadata; }

private:
    friend CommitBuilder;
    friend VersionController;

    CommitHash _hash;
    std::vector<std::shared_ptr<DataPart>> _dataparts;
    GraphMetadata* _graphMetadata {nullptr};
};

}

