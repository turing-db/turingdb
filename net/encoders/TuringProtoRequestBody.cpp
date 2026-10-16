#include "TuringProtoRequestBody.h"

#include <string.h>

#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"

#include "TuringException.h"

using namespace net::proto;

TuringProtoRequestBody::TuringProtoRequestBody()
    : _suffix(DEFAULT_BUFFER_CAPACITY),
    _parameterWriter(&_suffix)
{
    _suffix.setOnBufferFullCallBack([]() {
        throw TuringException(fmt::format("The query parameters exceed {} bytes", DEFAULT_BUFFER_CAPACITY));
    });
}

TuringProtoRequestBody::~TuringProtoRequestBody() {
}

void TuringProtoRequestBody::build(std::string_view query, const db::ParameterMap* parameters) {
    if (query.size() > MAX_WIRE_SIZE) {
        throw TuringException(fmt::format("The query is {} bytes, more than the {} bytes the binary protocol allows",
                                          query.size(),
                                          MAX_WIRE_SIZE));
    }

    const WireSize queryLength = static_cast<WireSize>(query.size());
    memcpy(_prefix.data(), &queryLength, sizeof(queryLength));

    _suffix.reset();
    _suffix.increaseOffset(sizeof(WireSize));

    const bool hasParameters = parameters && !parameters->empty();
    if (hasParameters) {
        _parameterWriter.writeParameters(*parameters);
    }

    const WireSize parameterLength = static_cast<WireSize>(_suffix.size() - sizeof(WireSize));
    memcpy(_suffix.data(), &parameterLength, sizeof(parameterLength));
}
