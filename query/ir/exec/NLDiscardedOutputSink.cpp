#include "NLDiscardedOutputSink.h"

using namespace db;

NLDiscardedOutputSink::NLDiscardedOutputSink()
{
}

NLDiscardedOutputSink::~NLDiscardedOutputSink() {
}

void NLDiscardedOutputSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
}
