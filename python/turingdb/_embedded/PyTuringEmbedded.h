#pragma once

#include <nanobind/nanobind.h>

#include <stdint.h>
#include <memory>
#include <string>

#include "QueryConfig.h"
#include "TuringConfig.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace db {
class LocalMemory;
class TuringDB;
}

namespace pybindings {

namespace nb = nanobind;

class PyTuringEmbedded {
public:
    explicit PyTuringEmbedded();
    explicit PyTuringEmbedded(const std::string& dataDir);
    ~PyTuringEmbedded();

    void setGraphName(const std::string& name) { _graphName = name; }
    const std::string& getGraphName() const { return _graphName; }

    void setChangeID(uint64_t v) { _changeID = db::ChangeID(v); }
    void clearChangeID() { _changeID = db::ChangeID::head(); }

    void setCommitHash(const std::string& s);
    void clearCommitHash() { _commitHash = db::CommitHash::head(); }

    nb::dict query(const std::string& cypher);

private:
    void init();

    db::TuringConfig _config;
    db::QueryConfig _queryConfig;
    std::unique_ptr<db::LocalMemory> _localMem;
    std::unique_ptr<db::TuringDB> _db;

    std::string _graphName {"default"};
    db::CommitHash _commitHash {db::CommitHash::head()};
    db::ChangeID _changeID {db::ChangeID::head()};
};

}
