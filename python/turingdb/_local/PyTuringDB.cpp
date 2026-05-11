#include "PyTuringDB.h"

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "NanobindUtils.h"

#include "LocalMemory.h"
#include "Path.h"
#include "QueryCallbacks.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "TuringException.h"

namespace pybindings {

PyTuringDB::PyTuringDB()
    : _localMem(std::make_unique<db::LocalMemory>())
{
    init();
}

PyTuringDB::PyTuringDB(const std::string& dataDir)
    : _localMem(std::make_unique<db::LocalMemory>())
{
    _config.setTuringDirectory(fs::Path(dataDir));
    init();
}

PyTuringDB::~PyTuringDB() = default;

void PyTuringDB::init() {
    _db = std::make_unique<db::TuringDB>(&_config);
    _db->init();
}

void PyTuringDB::setCommitHash(const std::string& s) {
    const auto res = db::CommitHash::fromString(s);
    if (!res.has_value()) {
        throw TuringException("Invalid commit hash: " + std::string(res.error()));
    }
    _commitHash = res.value();
}

nb::dict PyTuringDB::query(const std::string& cypher) {
    db::DataframeManager dfMan;
    db::Dataframe bufferedDf;
    // Owning storage for column names — see allocColumns docs.
    std::vector<std::string> nameStorage;
    bool columnsAlloced = false;
    db::LocalMemory* localMem = _localMem.get();

    const auto onData = [&bufferedDf, &nameStorage, &columnsAlloced, &dfMan, localMem](const db::Dataframe* df) {
        if (!columnsAlloced) {
            allocColumns(df, &bufferedDf, &dfMan, localMem, &nameStorage);
            columnsAlloced = true;
        }
        appendDfs(df, &bufferedDf);
    };

    db::QueryCallbacks callbacks;
    callbacks.setOnOutputData(onData);

    const db::QueryState state(_graphName,
                               _localMem.get(),
                               &_queryConfig,
                               &callbacks,
                               _commitHash,
                               _changeID);

    const db::QueryStatus status = _db->query(cypher, state);
    if (!status.isOk()) {
        // Format errors as "<STATUS>: <message>" to match the binary/HTTP path.
        throw TuringException(std::string(db::QueryStatusDescription::value(status.getStatus())) + ": " + status.getError());
    }

    nb::dict envelope = dataframeToNumpy(&bufferedDf);
    envelope["time"] = nb::cast(status.getTotalTime().count());
    return envelope;
}

}
