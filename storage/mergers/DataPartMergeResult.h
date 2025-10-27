#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class DataPartMergeErrorType : uint8_t {
    MERGE_GRAPH_FAILED,
    CHANGES_ON_MAIN,
    _SIZE,
};

using DataPartMergeErrorTypeDescription = EnumToString<DataPartMergeErrorType>::Create<
    EnumStringPair<DataPartMergeErrorType::MERGE_GRAPH_FAILED, "Could Not Build The Merged Graph">,
    EnumStringPair<DataPartMergeErrorType::CHANGES_ON_MAIN, "Could Not Merge Graph With Changes On Main">>;

class DataPartMergeError {
public:
    explicit DataPartMergeError(DataPartMergeErrorType type)
        : _type(type)
    {
    }

    [[nodiscard]] DataPartMergeErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<DataPartMergeError> result(DataPartMergeErrorType type) {
        return BadResult<DataPartMergeError>(DataPartMergeError(type));
    }

private:
    DataPartMergeErrorType _type {};
};

template <typename T>
using DataPartMergeResult = BasicResult<T, class DataPartMergeError>;

}
