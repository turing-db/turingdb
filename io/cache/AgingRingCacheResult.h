#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class AgingRingCacheErrorCode : uint8_t {
    UNKNOWN = 0,
    TERMINATING,
    COULD_NOT_LOAD,
    NOTHING_TO_EVICT,

    _SIZE,
};

using AgingRingCacheErrorTypeDescription = EnumToString<AgingRingCacheErrorCode>::Create<
    EnumStringPair<AgingRingCacheErrorCode::UNKNOWN, "Unknown">,
    EnumStringPair<AgingRingCacheErrorCode::TERMINATING, "Terminating">,
    EnumStringPair<AgingRingCacheErrorCode::COULD_NOT_LOAD, "Could not load entry">,
    EnumStringPair<AgingRingCacheErrorCode::NOTHING_TO_EVICT, "Nothing to evict">>;

class AgingRingCacheError {
public:
    explicit AgingRingCacheError(AgingRingCacheErrorCode type)
        : _type(type)
    {
    }

    [[nodiscard]] AgingRingCacheErrorCode getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<AgingRingCacheError> result(AgingRingCacheErrorCode type) {
        return BadResult<AgingRingCacheError>(AgingRingCacheError(type));
    }

private:
    AgingRingCacheErrorCode _type {AgingRingCacheErrorCode::UNKNOWN};
};

template <typename T>
using AgingRingCacheResult = BasicResult<T, class AgingRingCacheError>;

}
