#include "TuringProtoServerNlSink.h"

#include "TuringProtoWriter.h"

#include "BioAssert.h"

using namespace db;

TuringProtoServerNlSink::TuringProtoServerNlSink(net::proto::TuringProtoWriter* writer)
    : _writer(writer)
{
}

TuringProtoServerNlSink::~TuringProtoServerNlSink() {
}

void TuringProtoServerNlSink::declareOutput(std::span<const std::string_view> names,
                                            std::span<const Column* const> chunks) {
    bioassert(names.size() == chunks.size(), "The wire header needs one name per output column");

    _writer->writeColumnHeaders(names, chunks);
}

void TuringProtoServerNlSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    if (rowCount == 0) {
        return;
    }

    _writer->writeColumns(chunks, offset, rowCount);
}
