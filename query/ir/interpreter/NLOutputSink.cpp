#include "NLOutputSink.h"

using namespace db;

NLOutputSink::~NLOutputSink() {
}

void NLOutputSink::declareOutput(std::span<const std::string_view> names,
                                 std::span<const Column* const> chunks) {
}
