#include "PyTuringClient.h"

#include <stdint.h>
#include <memory>
#include <string>

#include "PyColumnContainer.h"
#include "PyParameters.h"
#include "PyResultBuilder.h"
#include "PySink.h"

#include "LocalMemory.h"
#include "ParameterMap.h"
#include "QueryStatus.h"

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

nb::dict PyTuringClient::query(const std::string& cypher, nb::handle parameters) {
    db::ParameterMap parameterMap;
    const bool hasParameters = !parameters.is_none();
    if (hasParameters) {
        readParameters(parameters, parameterMap);
    }

    PyColumnContainer container;
    PySink sink(&_values);
    PyResultBuilder builder(&container);

    const auto onChunkDecoded = [this, &builder]() {
        builder.appendChunk();
        _values.clearValues();
    };

    db::QueryStatus status;
    {
        nb::gil_scoped_release release;
        status = _client->sendQuery(cypher, &sink, &container, onChunkDecoded, hasParameters ? &parameterMap : nullptr);
    }

    if (!status.isOk()) {
        throw TuringException(std::string(db::QueryStatusDescription::value(status.getStatus())) + ": " + status.getError());
    }

    nb::dict envelope;
    builder.build(envelope);
    envelope["time"] = nb::cast(status.getTotalTime().count());
    return envelope;
}

}
