#include "TuringProtoEncoder.h"

#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "QueryStatus.h"

#include "BioAssert.h"

using namespace net::proto;

TuringProtoEncoder::TuringProtoEncoder(net::proto::TuringProtoOutBuf* outBuf)
    : _outBuf(outBuf)
{
}

void TuringProtoEncoder::writeDataframeHeader(const db::Dataframe* df) {
    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, ColumnHeaderWriter, db::OutputtedTypes::Excluded>;

    size_t chunkHeaderLen = sizeof(WireSize);
    for (const db::NamedColumn* namedCol : df->cols()) {
        chunkHeaderLen += net::proto::ColumnWireHeader::wireSize();
        chunkHeaderLen += namedCol->getName().size();
    }
    bioassert(chunkHeaderLen <= _outBuf->capacity(), "Dataframe schema exceeds maximum chunk size");

    bioassert(df->cols().size() <= MAX_WIRE_SIZE, "Number of columns exceed maximum number of columns");
    const WireSize columnCount = static_cast<WireSize>(df->cols().size());
    _outBuf->copyFixedLenData(&columnCount, sizeof(columnCount));

    for (const db::NamedColumn* namedCol : df->cols()) {
        const std::string_view colName = namedCol->getName();
        bioassert(colName.size() <= MAX_WIRE_SIZE, "Column name length exceeds maximum wire size");
        net::proto::ColumnWireHeader columnHeader {
            ._nameLen = static_cast<WireSize>(colName.size()),
            ._typeCode = 0,
            ._encoding = std::to_underlying(net::proto::ColumnKind::VECTOR)};

        ColumnHeaderWriter writer(columnHeader);
        const db::Column* col = namedCol->getColumn();
        Encoder::dispatch(col, writer);
        _outBuf->copyHeader(&columnHeader);
        _outBuf->copyVarLenData(colName.data(), colName.size());
    }
}

void TuringProtoEncoder::writeDataframe(const db::Dataframe* df) {
    if (df->getLogicalRowCount() == 0) {
        return;
    }

    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, DataWriter, db::OutputtedTypes::Excluded>;

    DataWriter writer(_outBuf, _stack);

    for (const db::NamedColumn* namedCol : df->cols()) {
        const db::Column* col = namedCol->getColumn();
        Encoder::dispatch(col, writer);
    }
}

void TuringProtoEncoder::writeError(const db::QueryStatus* status) {
    const auto statusCode = status->getStatus();

    _outBuf->copyFixedLenData(&statusCode, sizeof(statusCode));
    _outBuf->copyVarLenData(status->getError().data(), status->getError().size());
}

void TuringProtoEncoder::writeProtocolError(std::string_view message) {
    _outBuf->copyVarLenData(message.data(), message.size());
}

void TuringProtoEncoder::writeEnd(db::QueryCallbacks::ExecTimeMilliseconds milliseconds) {
    _outBuf->copyFixedLenData(&milliseconds, sizeof(milliseconds));
}
