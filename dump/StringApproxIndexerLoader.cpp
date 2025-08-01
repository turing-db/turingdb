#include "StringApproxIndexerLoader.h"
#include "DumpResult.h"
#include "PropertyContainerDumpConstants.h"

using namespace db;

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringApproxIndexerLoader::load() {
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

    return {};
}
