#pragma once

#include <memory>

#include "DataPartSpan.h"

namespace db {
class CommitBuilder;
class DataPartBuilder;
class JobSystem;

class DataPartMerger {
public:
    explicit DataPartMerger(CommitBuilder* commitBuilder);

    std::unique_ptr<DataPartBuilder> merge(DataPartSpan dataParts, JobSystem& jobsystem) const;

private:
    CommitBuilder* _commitBuilder;
};

}
