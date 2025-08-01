#include "StringApproxIndexerDumper.h"

#include "BioAssert.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "ID.h"
#include "PropertyContainerDumpConstants.h"
#include "TuringException.h"
#include "spdlog/spdlog.h"
#include <cstddef>
#include <string>

using namespace db;

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    using Constants = StringPropertyContainerDumpConstants;

    _writer.nextPage();

    const size_t propCount = idxer.size();

    std::vector<EntityID> ids;
    ids.reserve(propCount);

    // IDs
    {
        const uint64_t idPageCount = GraphDumpHelper::getPageCountForItems(
            propCount, Constants::ID_COUNT_PER_PAGE);
        const size_t remainder = propCount % Constants::ID_COUNT_PER_PAGE;

        // Get the IDs in its own vec to bulk write
        for (const auto& [id, _] : idxer) {
            ids.emplace_back(id.getValue());
        }

        size_t offset {0};
        for (size_t i = 0; i < idPageCount; i++) {
            _writer.nextPage();

            const bool isLastPage = (i == idPageCount - 1);
            const size_t countInPage =
                isLastPage ? (remainder == 0 ? Constants::ID_COUNT_PER_PAGE : remainder)
                           : Constants::ID_COUNT_PER_PAGE;

            const std::span idSpan = std::span {ids}.subspan(offset, countInPage);
            offset += countInPage;

            // Header
            _writer.writeToCurrentPage(countInPage);

            // Data
            for (const auto& id : idSpan) {
                _writer.writeToCurrentPage(id.getValue());
            }
        }
    }

    std::vector<size_t> numStringsPerId;
    numStringsPerId.reserve(ids.size());

    std::vector<std::vector<EntityID>> owners; // Build now, use later
    // NOTE: Is it worth using StringBuckets instead?
    // String values
    {
        std::vector<std::string> strings;
        std::vector<size_t> stringSizes;

        msgbioassert(ids.size() == numStringsPerId.size(),
                     "ID to string count calculation is wrong");

        for (const auto& id : ids) {
            size_t num_strings{0};
            for (num_strings++; const auto& it : *idxer.at(id.getValue())) {
                strings.emplace_back(it.word);
                stringSizes.emplace_back(it.word.size());

                // Populate the owners array
                for (const auto& owner : it.owners) {
                    owners.emplace_back(owner.getValue());
                }
            }
            numStringsPerId.push_back(num_strings);
        }

        size_t i {0};
        /* Format:
         * # of strings for this ID
         * [string size, string]
         * [string size, string]
         * ...
         */
        for (const auto& numStrings : numStringsPerId) {
            // Ensure there is space to write the number of strings for this property ID
            if (!ensureSpace(sizeof(size_t))) {
                spdlog::error("Couldn't write num strings");
                return DumpError::result(DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO);
            }
            // Write the total number of strings for this property ID
            _writer.writeToCurrentPage(numStrings);

            for (size_t j = i; j < i + numStrings; j++) {
                size_t thisStringDataSize =
                    sizeof(size_t) + (stringSizes.at(j) * sizeof(char));

                if (!ensureSpace(thisStringDataSize)) {
                    spdlog::error("String data size for property {} with string {} is "
                                  "too large to fit a single page");
                    return DumpError::result(
                        DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO);
                }

                // Write Size:String pair
                _writer.writeToCurrentPage(stringSizes.at(j));
                _writer.writeToCurrentPage(strings.at(j));
            }
            i += numStrings;
        }
    }

    // Owner information
    {
        size_t i {0};
        /* Format:
         * # of strings for this ID
         * [owners vector size, owners vector]
         * [owners vector size, owners vector]
         * ...
         */
        for (const auto& numStrings : numStringsPerId) {
            // Ensure there is space to write the number of strings for this property ID
            if (!ensureSpace(sizeof(size_t))) {
                spdlog::error("Couldn't write num strings");
                return DumpError::result(DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO);
            }
            // Write the total number of strings for this property ID
            _writer.writeToCurrentPage(numStrings);

            // For each string entry
            for (size_t j = i; j < i + numStrings; j++) {
                // Ensure all the data for owner info can fit on this page
                size_t thisOwnersDataSize =
                    sizeof(size_t) + (owners.at(j).size() * sizeof(size_t));
                if (!ensureSpace(thisOwnersDataSize)) {
                    spdlog::error(
                        "Owners size for property {} is too large to fit a single page");
                    return DumpError::result(
                        DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO);
                }

                size_t before = _writer.getBytesWritten();

                // Write Size:ownervec pair
                _writer.writeToCurrentPage(owners.at(j).size());
                for (const auto& owner : owners.at(j)) {
                    _writer.writeToCurrentPage(owner.getValue());
                }

                size_t after = _writer.getBytesWritten();
                if (after - before != thisOwnersDataSize) {
                    throw TuringException("Incorrect calculation of owners size");
                }
            }
            i += numStrings;
        }
    }
    return {};
}

/* Loader algorithm:
- Load all the IDs, initialiseIndexTrie for all of them
- Load all the strings, associated by ID       (a)
- Load all the owner vectors, associated by ID (b)
- For each propID: insert into the index with (a), (b)
*/


bool StringApproxIndexerDumper::ensureSpace(size_t requiredSpace) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        return false;
    }
    if (_writer.buffer().avail() < requiredSpace) {
        _writer.nextPage();
    }
    return true;
}
