#pragma once

#include "metadata/LabelSetMap.h"
#include "FilePageReader.h"
#include "GraphDumpHelper.h"

namespace db {

class LabelSetMapLoader {
public:
    static constexpr size_t METADATA_PAGE_STRIDE = DumpConfig::FILE_HEADER_STRIDE
                                                 + sizeof(uint64_t)  // LabelSet::IntegerSize
                                                 + sizeof(uint64_t)  // LabelSet::IntegerCount
                                                 + sizeof(uint64_t)  // LabelSet count
                                                 + sizeof(uint64_t); // Page count

    static constexpr size_t LABELSET_STRIDE = sizeof(LabelSet);

    explicit LabelSetMapLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(LabelSetMap& labelsets) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() < METADATA_PAGE_STRIDE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        if (it.get<uint64_t>() != LabelSet::IntegerSize) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
        }

        if (it.get<uint64_t>() != LabelSet::IntegerCount) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
        }

        const uint64_t labelsetsCount = it.get<uint64_t>();
        const uint64_t pageCount = it.get<uint64_t>();
        LabelSet labelset;
        auto* labelsetData = labelset.data();

        for (size_t i = 0; i < pageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() < DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                if (it.remainingBytes() < LABELSET_STRIDE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
                }

                for (size_t k = 0; k < LabelSet::IntegerCount; k++) {
                    labelsetData[k] = it.get<LabelSet::IntegerType>();
                }
                labelsets.getOrCreate(labelset);
            }
        }

        if (labelsetsCount != labelsets.getCount()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
        }

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
