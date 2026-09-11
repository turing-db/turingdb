#include "TuringProtoEncoder.h"

#include <ranges>

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
    const db::Dataframe::NamedColumns& columns = df->cols();

    size_t chunkHeaderLen = sizeof(WireSize);
    for (const db::NamedColumn* namedColumn : columns) {
        chunkHeaderLen += net::proto::ColumnWireHeader::wireSize();
        chunkHeaderLen += namedColumn->getName().size();
    }
    bioassert(chunkHeaderLen <= _outBuf->capacity(), "Dataframe schema exceeds maximum chunk size");

    writeColumnCount(columns.size());

    for (const db::NamedColumn* namedColumn : columns) {
        writeColumnHeader(namedColumn->getName(), namedColumn->getColumn());
    }
}

void TuringProtoEncoder::writeColumnHeaders(std::span<const std::string_view> names,
                                            std::span<const db::Column* const> cols) {
    size_t chunkHeaderLen = sizeof(WireSize);
    for (const std::string_view name : names) {
        chunkHeaderLen += net::proto::ColumnWireHeader::wireSize();
        chunkHeaderLen += name.size();
    }
    bioassert(chunkHeaderLen <= _outBuf->capacity(), "Column schema exceeds maximum chunk size");

    writeColumnCount(cols.size());

    for (const auto [name, column] : std::views::zip(names, cols)) {
        writeColumnHeader(name, column);
    }
}

void TuringProtoEncoder::writeDataframe(const db::Dataframe* df) {
    const size_t rowCount = df->getLogicalRowCount();
    if (rowCount == 0) {
        return;
    }

    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, DataWriter, db::OutputtedTypes::Excluded>;

    DataWriter writer(_outBuf, _stack, 0, rowCount);
    for (const db::NamedColumn* namedColumn : df->cols()) {
        Encoder::dispatch(namedColumn->getColumn(), writer);
    }
}

void TuringProtoEncoder::writeColumns(std::span<const db::Column* const> cols, size_t offset, size_t rowCount) {
    if (rowCount == 0) {
        return;
    }

    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, DataWriter, db::OutputtedTypes::Excluded>;

    DataWriter writer(_outBuf, _stack, offset, rowCount);
    for (const db::Column* column : cols) {
        Encoder::dispatch(column, writer);
    }
}

void TuringProtoEncoder::writeColumnCount(size_t count) {
    bioassert(count <= MAX_WIRE_SIZE, "Number of columns exceed maximum number of columns");
    const WireSize columnCount = static_cast<WireSize>(count);
    _outBuf->copyFixedLenData(&columnCount, sizeof(columnCount));
}

void TuringProtoEncoder::writeColumnHeader(std::string_view name, const db::Column* column) {
    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, ColumnHeaderWriter, db::OutputtedTypes::Excluded>;

    bioassert(name.size() <= MAX_WIRE_SIZE, "Column name length exceeds maximum wire size");
    net::proto::ColumnWireHeader columnHeader {
        ._nameLen = static_cast<WireSize>(name.size()),
        ._typeCode = 0,
        ._encoding = std::to_underlying(net::proto::ColumnKind::VECTOR)};

    ColumnHeaderWriter writer(columnHeader);
    Encoder::dispatch(column, writer);
    _outBuf->copyHeader(&columnHeader);
    _outBuf->copyVarLenData(name.data(), name.size());
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
