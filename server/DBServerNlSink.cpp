#include "DBServerNlSink.h"

#include "BioAssert.h"

using namespace db;

DBServerNlSink::DBServerNlSink(JsonEncoder<net::NetWriter>* encoder)
    : _encoder(encoder)
{
}

DBServerNlSink::~DBServerNlSink() {
}

void DBServerNlSink::declareOutput(std::span<const std::string_view> names,
                                   std::span<const Column* const> chunks) {
    bioassert(names.size() == chunks.size(), "The JSON header needs one name per output column");

    _encoder->writeColumnHeaders(names, chunks);
}

void DBServerNlSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    if (rowCount == 0) {
        return;
    }

    _encoder->writeColumns(chunks, offset, rowCount);
}
