#pragma once

#include <stdint.h>
#include <algorithm>
#include <charconv>
#include <span>
#include <type_traits>

#include "ID.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "FileReader.h"
#include "FileWriter.h"
#include "ByteBufferIterator.h"

namespace db {

class GraphDumpHelper {
public:
    [[nodiscard]] static constexpr size_t getPageCountForItems(size_t itemCount, size_t itemsPerPage) {
        return itemCount / itemsPerPage + ((itemCount % itemsPerPage) != 0);
    }

    // Pages an array of entity IDs, header-count then raw ID bytes per page. EntityID is
    // a standard-layout uint64_t wrapper, layout pinned in PropertyContainerDumpConstants.h
    static void writeEntityIDPages(fs::FilePageWriter& writer,
                                   std::span<const EntityID> ids,
                                   size_t idCountPerPage) {
        const size_t stride = sizeof(EntityID::Type);
        const size_t pageCount = getPageCountForItems(ids.size(), idCountPerPage);
        const size_t remainder = ids.size() % idCountPerPage;

        const uint8_t* idBytes = reinterpret_cast<const uint8_t*>(ids.data());

        size_t offset = 0;
        for (size_t i = 0; i < pageCount; i++) {
            writer.nextPage();

            const bool isLastPage = (i == pageCount - 1);
            const size_t countInPage = isLastPage
                                         ? (remainder == 0 ? idCountPerPage : remainder)
                                         : idCountPerPage;

            writer.writeToCurrentPage(countInPage);
            writer.writeToCurrentPage(std::span {idBytes + offset * stride, countInPage * stride});

            offset += countInPage;
        }
    }

    template <typename T>
    [[nodiscard]] static constexpr size_t getCountPerPage() {
        return (DumpConfig::PAGE_SIZE - sizeof(uint64_t)) / sizeof(T);
    }

    template <typename T>
    [[nodiscard]] static DumpResult<void> loadPage(fs::FilePageReader& reader,
                                                   std::span<T> out,
                                                   size_t& countInPage,
                                                   DumpErrorType errorType) {
        static_assert(std::is_trivially_copyable_v<T>);

        reader.nextPage();

        if (reader.errorOccured()) {
            return DumpError::result(errorType, reader.error().value());
        }

        fs::AlignedBufferIterator it = reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(errorType);
        }

        const uint64_t count = it.get<uint64_t>();
        const bool overflowsPage = count > getCountPerPage<T>();
        const bool overflowsOut = count > out.size();
        if (overflowsPage || overflowsOut) {
            return DumpError::result(errorType);
        }

        std::copy_n(it.data(), count * sizeof(T), reinterpret_cast<uint8_t*>(out.data()));
        countInPage = count;

        return {};
    }

    template <typename T>
    [[nodiscard]] static DumpResult<void> loadPages(fs::FilePageReader& reader,
                                                    size_t pageCount,
                                                    std::span<T> out,
                                                    DumpErrorType errorType) {
        size_t offset = 0;
        for (size_t i = 0; i < pageCount; i++) {
            size_t countInPage = 0;
            if (auto res = loadPage(reader, out.subspan(offset), countInPage, errorType); !res) {
                return res;
            }

            offset += countInPage;
        }

        if (offset != out.size()) {
            return DumpError::result(errorType);
        }

        return {};
    }

    static void writeFileHeader(fs::FilePageWriter& writer) {
        writer.writeToCurrentPage(DumpConfig::ONE_BAD_CAFE);
        writer.writeToCurrentPage(DumpConfig::VERSION);
    }

    static void writeFileHeader(fs::FileWriter<>* writer) {
        writer->write(DumpConfig::ONE_BAD_CAFE);
        writer->write(DumpConfig::VERSION);
    }

    static DumpResult<uint64_t> checkFileHeader(fs::AlignedBufferIterator& it) {
        const auto badcafe = it.get<decltype(DumpConfig::ONE_BAD_CAFE)>();
        const auto version = it.get<decltype(DumpConfig::VERSION)>();

        if (badcafe != DumpConfig::ONE_BAD_CAFE) {
            return DumpError::result(DumpErrorType::NOT_TURING_FILE);
        }

        if (version < DumpConfig::UP_TO_DATE_VERSION) {
            return DumpError::result(DumpErrorType::OUTDATED);
        }

        return version;
    }

    static DumpResult<void> checkFileHeader(fs::ByteBufferIterator* it) {
        const auto badcafe = it->get<decltype(DumpConfig::ONE_BAD_CAFE)>();
        const auto version = it->get<decltype(DumpConfig::VERSION)>();

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

        uint64_t integer = 0;
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
        uint64_t offset = 0;
        const auto offsetRes = std::from_chars(offsetStr.begin(), offsetStr.end(), offset);
        const size_t offsetSize = std::distance(offsetStr.data(), offsetRes.ptr);

        const bool offsetFailure = (offsetRes.ec == std::errc::result_out_of_range)
                                || (offsetRes.ec == std::errc::invalid_argument);

        if (offsetFailure) {
            return DumpError::result(DumpErrorType::UNKNOWN);
        }

        // Parsing hash
        uint64_t hash = 0;
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
