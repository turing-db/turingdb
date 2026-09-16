#include "PyTuringEmbedded.h"

#include <stdint.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NanobindUtils.h"

#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "Path.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "TuringException.h"

namespace pybindings {

namespace {

class PyEmbeddedNLSink : public db::NLOutputSink {
public:
    PyEmbeddedNLSink(db::Dataframe* bufferedDf,
                     db::DataframeManager* dfMan,
                     db::LocalMemory* localMem)
        : _bufferedDf(bufferedDf),
        _dfMan(dfMan),
        _localMem(localMem)
    {
    }

    void declareOutput(std::span<const std::string_view> names,
                       std::span<const db::Column* const> chunks) override {
        allocChunkColumns(names, chunks, _bufferedDf, _dfMan, _localMem, &_nameStorage);
    }

    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override {
        appendChunkColumns(chunks, offset, rowCount, _bufferedDf);
    }

private:
    db::Dataframe* _bufferedDf {nullptr};
    db::DataframeManager* _dfMan {nullptr};
    db::LocalMemory* _localMem {nullptr};
    // Owning storage for column names - see allocColumns docs.
    std::vector<std::string> _nameStorage;
};

}

PyTuringEmbedded::PyTuringEmbedded()
    : _localMem(std::make_unique<db::LocalMemory>())
{
    init();
}

PyTuringEmbedded::PyTuringEmbedded(const std::string& dataDir)
    : _localMem(std::make_unique<db::LocalMemory>())
{
    _config.setTuringDirectory(fs::Path(dataDir));
    init();
}

PyTuringEmbedded::~PyTuringEmbedded() = default;

void PyTuringEmbedded::init() {
    _db = std::make_unique<db::TuringDB>(&_config);
    _db->init();
}

void PyTuringEmbedded::setCommitHash(const std::string& s) {
    const auto res = db::CommitHash::fromString(s);
    if (!res.has_value()) {
        throw TuringException("Invalid commit hash: " + std::string(res.error()));
    }
    _commitHash = res.value();
}

nb::dict PyTuringEmbedded::query(const std::string& cypher) {
    db::DataframeManager dfMan;
    db::Dataframe bufferedDf;
    PyEmbeddedNLSink sink(&bufferedDf, &dfMan, _localMem.get());

    const db::QueryState state(_graphName,
                               _localMem.get(),
                               &_queryConfig,
                               &sink,
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
