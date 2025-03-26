#pragma once

#include <vector>

#include "ArcManager.h"
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

    std::vector<WeakArc<const DataPart>> _allDataparts;
    std::vector<WeakArc<const DataPart>> _commitDataparts;
};

}
