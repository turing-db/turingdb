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

void PyTuringClient::setCommitHash(const std::string& commitHash) {
    const auto result = db::CommitHash::fromString(commitHash);
    if (!result.has_value()) {
        throw TuringException("Invalid commit hash: " + std::string(result.error()));
    }

    _client->setCommitHash(result.value());
}

nb::dict PyTuringClient::query(const std::string& cypher) {
    db::DataframeManager dataframeManager;
    db::Dataframe bufferedDataframe;
    // Owning storage for column names — see allocColumns docs.
    std::vector<std::string> nameStorage;
    bool columnsAllocated = false;
    db::LocalMemory* localMemory = _localMem.get();

    const auto onData = [&bufferedDataframe, &nameStorage, &columnsAllocated, &dataframeManager, localMemory](const db::Dataframe* dataframe) {
        if (!columnsAllocated) {
            allocColumns(dataframe, &bufferedDataframe, &dataframeManager, localMemory, &nameStorage);
            columnsAllocated = true;
        }
        appendDfs(dataframe, &bufferedDataframe);
    };

    const db::QueryStatus status = _client->sendQuery(cypher, onData);
    if (!status.isOk()) {
        throw TuringException(std::string(db::QueryStatusDescription::value(status.getStatus())) + ": " + status.getError());
    }

    nb::dict envelope = dataframeToNumpy(&bufferedDataframe);
    envelope["time"] = nb::cast(status.getTotalTime().count());
    return envelope;
}

}
