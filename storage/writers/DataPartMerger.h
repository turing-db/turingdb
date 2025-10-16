#pragma once

#include "DataPartSpan.h"

namespace db {
class CommitBuilder;
class VersionController;
class JobSystem;
class GraphMetadata;

class DataPartMerger {
public:
    DataPartMerger(VersionController* versionController,
                   CommitBuilder* commitBuilder,
                   GraphMetadata* graphMetadata);

    void merge(DataPartSpan dataParts, JobSystem& jobsystem);

private:
    VersionController* _versionController;
    CommitBuilder* _commitBuilder;
    GraphMetadata* _graphMetadata;
};

}
