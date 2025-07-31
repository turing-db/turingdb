#include "StringApproxIndexerDumper.h"
#include "DumpResult.h"
#include "FilePageWriter.h"

using namespace db;

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    _writer.nextPage();

    const size_t indexSize =  idxer.size();

    _writer.write(indexSize);

    for (const auto& [propID, idx] : idxer) {
        for (const auto& it : *idx) {
        }
    }


    return {};
}
