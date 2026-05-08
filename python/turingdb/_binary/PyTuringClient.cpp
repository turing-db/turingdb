#include "PyTuringClient.h"

#include <stdint.h>
#include <memory>
#include <string>

#include "NanobindUtils.h"

#include "LocalMemory.h"
#include "QueryStatus.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "TuringException.h"

namespace pybindings {

PyTuringClient::PyTuringClient(const std::string& host, const std::string& port)
    : _localMem(std::make_unique<db::LocalMemory>()),
    _client(std::make_unique<net::proto::TuringClient>(host, port, _localMem.get()))
{
}

PyTuringClient::~PyTuringClient() = default;

void PyTuringClient::setCommitHash(const std::string& s) {
    const auto res = db::CommitHash::fromString(s);
    if (!res.has_value()) {
        throw TuringException("Invalid commit hash: " + std::string(res.error()));
    }

    _client->setCommitHash(res.value());
}

nb::dict PyTuringClient::query(const std::string& cypher) {
    db::DataframeManager dfMan;
    db::Dataframe bufferedDf;
    // Owning storage for column names — see allocColumns docs.
    std::vector<std::string> nameStorage;
    bool columnsAlloced = false;
    db::LocalMemory* localMem = _localMem.get();

    const auto cb = [&bufferedDf, &nameStorage, &columnsAlloced, &dfMan, localMem](const db::Dataframe* df) {
        if (!columnsAlloced) {
            allocColumns(df, &bufferedDf, &dfMan, localMem, &nameStorage);
            columnsAlloced = true;
        }
        appendDfs(df, &bufferedDf);
    };

    const db::QueryStatus status = _client->sendQuery(cypher, cb);
    if (!status.isOk()) {
        // Format errors as "<STATUS>: <message>" (e.g. "EXEC_ERROR: ...") to match
        // the HTTP transport, which prefixes server errors with the status enum name.
        throw TuringException(std::string(db::QueryStatusDescription::value(status.getStatus())) + ": " + status.getError());
    }

    nb::dict envelope = dataframeToNumpy(&bufferedDf);
    envelope["time"] = nb::cast(status.getTotalTime().count());
    return envelope;
}

}
