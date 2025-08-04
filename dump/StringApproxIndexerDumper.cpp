#include "StringApproxIndexerDumper.h"

#include "BioAssert.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "ID.h"
#include "PropertyContainerDumpConstants.h"
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

        spdlog::info("Dumping IDs");
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

    spdlog::info("Dumping string values");
    {

        // Calculate the number of entries per propertyID: info for loader
        std::vector<size_t> numEntries;
        numEntries.reserve(ids.size());

        for (const auto& id : ids) {
            auto& thisIndex = *idxer.at(id.getValue());
            size_t thisNumEntries {0};
            // TODO: Add distance support to prefix trie iterator
            for (thisNumEntries++; [[maybe_unused]] const auto& _ : thisIndex)
                ;
            numEntries.push_back(thisNumEntries);
        }

        msgbioassert(numEntries.size() == ids.size(),
                     "Error in entry-per-propID calculation");

        for (size_t i = 0; i < ids.size(); i++) {
            const auto& id = ids.at(i);
            for (const auto& it : *idxer.at(id.getValue())) {
                std::string string = it.word;
                auto& owners = it.owners;

                // XXX: Will never work if thisData exceeds page size
                // TODO: Split the checks up so that the data may span multiple pages
                size_t thisData = sizeof(size_t) + sizeof(size_t) // Num entries, str size
                                + sizeof(char) * string.size()    // string data
                                + sizeof(size_t)                  // owners size
                                + sizeof(size_t) * owners.size(); // owners data
                if (!ensureSpace(thisData)) {
                    return DumpError::result(
                        DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO);
                }
                _writer.writeToCurrentPage(numEntries.at(i));
                _writer.writeToCurrentPage(string.size());
                _writer.writeToCurrentPage(string);
                _writer.writeToCurrentPage(owners.size());
                for (const auto& ownerID : owners) {
                    _writer.writeToCurrentPage(ownerID.getValue());
                }
            }
        }
    }
    return {};
}

bool StringApproxIndexerDumper::ensureSpace(size_t requiredSpace) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        return false;
    }
    if (_writer.buffer().avail() < requiredSpace) {
        _writer.nextPage();
    }
    return true;
}
