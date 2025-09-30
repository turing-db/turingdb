#pragma once

#include "Profiler.h"
#include "indexers/PropertyIndexer.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "PropertyIndexerDumpConstants.h"
#include "metadata/GraphMetadata.h"

namespace db {

class PropertyIndexerLoader {
public:
    using Constants = PropertyIndexerDumperConstants;

    explicit PropertyIndexerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(const GraphMetadata& metadata,
                                        PropertyIndexer& indexer) {
        Profile profile {"PropertyIndexerLoader::load"};

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const uint64_t propTypeCount = it.get<uint64_t>();
        const uint64_t pageCount = it.get<uint64_t>();

        for (size_t i = 0; i < pageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                const auto ptID = it.get<PropertyTypeID::Type>();
                const auto lsetCount = it.get<uint64_t>();

                auto& ptIndexer = indexer.emplace(ptID, LabelSetPropertyIndexer {}).first->second;

                for (size_t k = 0; k < lsetCount; k++) {
                    const auto lsetID = it.get<LabelSetID::Type>();

                    const auto labelset = metadata.labelsets().getValue(lsetID);

                    if (!labelset) {
                        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER);
                    }

                    auto& info = ptIndexer[labelset.value()];

                    info.resize(it.get<uint64_t>());

                    for (auto& range : info) {
                        range._offset = it.get<uint64_t>();
                        range._count = it.get<uint64_t>();
                    }
                }
            }
        }

        if (propTypeCount != indexer.size()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER);
        }

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
