#pragma once

#include <memory>
#include <vector>

#include "DataPartSpan.h"

namespace db {

class DataPart;
class CommitBuilder;
class CommitLoader;
class GraphLoader;
class VersionController;

class CommitHistory {
public:
    DataPartSpan allDataparts() const { return _allDataparts; }
    DataPartSpan commitDataparts() const { return _commitDataparts; }

private:
    friend CommitBuilder;
    friend CommitLoader;
    friend GraphLoader;
    friend VersionController;

    std::vector<std::shared_ptr<const DataPart>> _allDataparts;
    std::vector<std::shared_ptr<const DataPart>> _commitDataparts;
};

}
