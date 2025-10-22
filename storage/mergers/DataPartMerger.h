#pragma once

#include "DataPartSpan.h"
#include <memory>

namespace db {
class CommitBuilder;
class VersionController;
class DataPartBuilder;
class JobSystem;
class GraphMetadata;

class DataPartMerger {
public:
    explicit DataPartMerger(CommitBuilder* commitBuilder);

    std::unique_ptr<DataPartBuilder> merge(DataPartSpan dataParts, JobSystem& jobsystem) const;

private:
    CommitBuilder* _commitBuilder;
};

}
