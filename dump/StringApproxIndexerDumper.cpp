#include "StringApproxIndexerDumper.h"

#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "ID.h"
#include "TuringException.h"

using namespace db;

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    const size_t indexSize = idxer.size();

    _writer.nextPage();

    // Write the number of string properties which are indexed
    _writer.write(indexSize);
    const size_t pageCount = indexSize;

    for (const auto& [propID, idx] : idxer) {
        if (!idx) {
            throw TuringException("nullptr string index");
        }
        // XXX: Should use a better method of separating properties. Delimiter?
        _writer.nextPage();

        // XXX: What happens if the below does not fit on a single page?

        // Declare what property we are now writing
        _writer.write(propID.getValue());
        for (const auto& it : *idx) {
            // Note the size of the string we are writing
            _writer.write(it.word.size());
            // Write the value of the string property
            _writer.write(it.word);
            // Note the number of IDs which follows
            _writer.write(it.owners.size());
            // Write the Nodes/Edges which have this property value
            for (const auto& id : it.owners) {
                _writer.write(id.getValue());
            }

        }
    }
    // Metadata
    _writer.seek(0);
    GraphDumpHelper::writeFileHeader(_writer);
    _writer.write(pageCount);

    if (_writer.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS,
                                 _writer.error().value());
    }

    return {};
}
