#pragma once

#include <charconv>

#include "DumpResult.h"
#include "FilePageWriter.h"
#include "DumpConfig.h"

namespace db {

class GraphDumpHelper {
public:
    [[nodiscard]] static constexpr size_t getPageCountForItems(size_t itemCount, size_t itemsPerPage) {
        return itemCount / itemsPerPage + ((itemCount % itemsPerPage) != 0);
    }

    static void writeFileHeader(fs::FilePageWriter& writer) {
        writer.writeToCurrentPage(DumpConfig::ONE_BAD_CAFE);
        writer.writeToCurrentPage(DumpConfig::VERSION);
    }

    static DumpResult<void> checkFileHeader(fs::AlignedBufferIterator& it) {
        const auto badcafe = it.get<decltype(DumpConfig::ONE_BAD_CAFE)>();
        const auto version = it.get<decltype(DumpConfig::VERSION)>();

        if (badcafe != DumpConfig::ONE_BAD_CAFE) {
            return DumpError::result(DumpErrorType::NOT_TURING_FILE);
        }

        if (version < DumpConfig::UP_TO_DATE_VERSION) {
            return DumpError::result(DumpErrorType::OUTDATED);
        }

        return {};
    }

    static DumpResult<uint64_t> getIntegerSuffix(std::string_view str, size_t prefixSize) {
        const std::string_view integerStr = str.substr(prefixSize);

        if (integerStr.empty()) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }

        uint64_t integer {0};
        const auto matchRes = std::from_chars(integerStr.begin(), integerStr.end(), integer);
        const size_t matchSize = std::distance(integerStr.data(), matchRes.ptr);

        const bool matchFailure = (matchRes.ec == std::errc::result_out_of_range)
                               || (matchRes.ec == std::errc::invalid_argument)
                               || (matchSize != integerStr.size());
        if (matchFailure) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }

        return integer;
    }

    static DumpResult<std::pair<uint64_t, uint64_t>> getCommitSuffix(std::string_view str, size_t prefixSize) {
        const std::string_view offsetStr = str.substr(prefixSize);

        if (offsetStr.empty()) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }
        // File name has the form commit-<offset>-<hash>

        // Parsing offset
        uint64_t offset {0};
        const auto offsetRes = std::from_chars(offsetStr.begin(), offsetStr.end(), offset);
        const size_t offsetSize = std::distance(offsetStr.data(), offsetRes.ptr);

        const bool offsetFailure = (offsetRes.ec == std::errc::result_out_of_range)
                                || (offsetRes.ec == std::errc::invalid_argument);

        if (offsetFailure) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }

        // Parsing hash
        uint64_t hash {0};
        const std::string_view hashStr = str.substr(prefixSize + offsetSize + 1);
        const auto hashRes = std::from_chars(hashStr.begin(), hashStr.end(), hash);
        const size_t hashSize = std::distance(hashStr.data(), hashRes.ptr);

        const bool hashFailure = (hashRes.ec == std::errc::result_out_of_range)
                              || (hashRes.ec == std::errc::invalid_argument)
                              || (hashSize != hashStr.size());

        if (hashFailure) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }

        return {std::make_pair(offset, hash)};
    }
};

}
