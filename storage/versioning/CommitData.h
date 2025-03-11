#pragma once

#include "versioning/CommitHash.h"
#include "versioning/CommitHistory.h"

namespace db {

class DataPart;
class CommitBuilder;
class CommitLoader;
class GraphLoader;
class GraphMetadata;
class VersionController;

class CommitData {
public:
    CommitHash hash() const { return _hash; }
    DataPartSpan allDataparts() const { return _history.allDataparts(); ;}
    DataPartSpan commitDataparts() const { return _history.commitDataparts(); ;}
    GraphMetadata& metadata() const { return *_graphMetadata; }

private:
    friend CommitBuilder;
    friend CommitLoader;
    friend GraphLoader;
    friend VersionController;

    CommitHash _hash = CommitHash::create();
    CommitHistory _history;
    GraphMetadata* _graphMetadata {nullptr};
};

}

