#pragma once

#include "metadata/LabelMap.h"
#include "FilePageReader.h"
#include "GraphDumpHelper.h"

namespace db {

class LabelMapLoader {
public:
    static constexpr size_t LABEL_BASE_STRIDE = sizeof(uint64_t); // String size

    explicit LabelMapLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(LabelMap& labels) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const uint64_t labelCount = it.get<uint64_t>();
        const uint64_t pageCount = it.get<uint64_t>();

        for (size_t i = 0; i < pageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() < DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                if (it.remainingBytes() < LABEL_BASE_STRIDE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
                }

                const uint64_t strsize = it.get<uint64_t>();

                if (it.remainingBytes() < strsize) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
                }

                const std::string_view name = it.get<char>(strsize);
                labels.getOrCreate(std::string {name});
            }
        }

        if (labelCount != labels.getCount()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
        }

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
