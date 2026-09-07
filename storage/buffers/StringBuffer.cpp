#include "StringBuffer.h"

#include <string.h>
#include <string_view>
#include <functional>

#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/view/drop.hpp>

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

std::string_view StringBuffer::concatenate(std::string_view a, std::string_view b) {
    const size_t totalSize = a.size() + b.size();

    _buf.reserveContiguous(totalSize);

    char* start = _buf.nextPtr();

    memcpy(start, a.data(), a.size());
    memcpy(start + a.size(), b.data(), b.size());

    _buf.commit(totalSize);

    return {start, totalSize};
}

std::string_view StringBuffer::join(std::span<const std::string_view> strs, std::string_view sep) {
    const size_t numElements = strs.size();

    if (numElements == 0) {
        return {_buf.nextPtr(), 0};
    }

    const size_t totalChars = [&] -> size_t {
        const size_t sepSize = sep.size();
        const size_t numSeps = numElements - 1;
        const size_t eleLen = rg::accumulate(strs, 0UZ, std::plus<> {}, rg::size);

        return eleLen + (sepSize * numSeps);
    }();

    _buf.reserveContiguous(totalChars);

    char* ptr = _buf.nextPtr();
    const char* start = ptr;

    const auto write = [&](std::string_view str) {
        memcpy(ptr, str.data(), str.size());
        ptr += str.size();
    };

    const std::string_view fst = strs.front();
    write(fst);

    for (const std::string_view next : strs | rv::drop(1)) {
        write(sep);
        write(next);
    }

    _buf.commit(totalChars);

    return {start, totalChars};
}
