#include "StringApproxIndexerLoader.h"
#include "DumpResult.h"
#include "PropertyContainerDumpConstants.h"
#include "indexers/StringPropertyIndexer.h"

using namespace db;

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringApproxIndexerLoader::load() { return {};}
/*
    using Constants = StringPropertyContainerDumpConstants;


    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                 _reader.error().value());
    }

    auto it = _reader.begin();

    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    uint64_t propCount = it.get<uint64_t>();
    uint64_t idPageCount = it.get<uint64_t>();
    auto index = new StringPropertyIndexer;
    // TODO : Resize index(propCount)

    size_t offset {0};
    for (size_t i = 0; i < idPageCount; i++) {
        _reader.nextPage();
        if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        it = _reader.begin();

        // Check that we read a whole page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }
        const size_t countInPage = it.get<uint64_t>();
        offset += countInPage;
    }


    return {};
}
*/
