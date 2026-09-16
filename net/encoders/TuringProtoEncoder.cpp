#include "TuringProtoEncoder.h"

#include <range/v3/view/zip.hpp>

#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "QueryStatus.h"

#include "BioAssert.h"

using namespace net::proto;
namespace rv = ranges::views;

TuringProtoEncoder::TuringProtoEncoder(net::proto::TuringProtoOutBuf* outBuf)
    : _outBuf(outBuf)
{
}

void TuringProtoEncoder::writeColumnHeaders(std::span<const std::string_view> names,
                                            std::span<const db::Column* const> columns) {
    bioassert(names.size() == columns.size(), "The wire schema needs one name per column");

    size_t chunkHeaderLen = sizeof(WireSize);
    for (const std::string_view name : names) {
        chunkHeaderLen += net::proto::ColumnWireHeader::wireSize();
        chunkHeaderLen += name.size();
    }
    bioassert(chunkHeaderLen <= _outBuf->capacity(), "Column schema exceeds maximum chunk size");

    writeColumnCount(columns.size());

    for (const auto [name, column] : rv::zip(names, columns)) {
        writeColumnHeader(name, column);
    }
}

void TuringProtoEncoder::writeColumns(std::span<const db::Column* const> columns, size_t offset, size_t rowCount) {
    if (rowCount == 0) {
        return;
    }

    using Encoder = db::ColumnSingleDispatcher<db::OutputtedTypes::Allowed, DataWriter, db::OutputtedTypes::Excluded>;

    DataWriter writer(_outBuf, _stack, offset, rowCount);
    for (const db::Column* column : columns) {
        Encoder::dispatch(column, writer);
    }
}

void TuringProtoEncoder::writeChunkFooter(size_t rowCount) {
    bioassert(rowCount <= MAX_WIRE_SIZE, "Chunk row count exceeds maximum wire size");
    const WireSize wireRowCount = static_cast<WireSize>(rowCount);
    _outBuf->copyFixedLenData(&wireRowCount, sizeof(wireRowCount));
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
