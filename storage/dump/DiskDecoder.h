#pragma once

#include "DumpResult.h"
#include "Path.h"

namespace db {

class GraphLoaderInterface;

/**
 * @brief Abstract interface for reading a TuringDB graph off disk.
 * @detail A DiskDecoder owns directory walking, file management, and byte-level decoding.
 * It never touches Graph/Commit/DataPart private state directly — instead it streams
 * decoded values through a host-supplied @ref GraphLoaderInterface that builds the
 * in-memory graph. This separation lets concrete decoders (potentially compiled to WASM)
 * remain decoupled from the storage layer's class layout, enabling runtime injection of
 * a format-specific decoder for backward / forward compatibility.
 */
class DiskDecoder {
public:
    explicit DiskDecoder(GraphLoaderInterface* loader)
        : _loader(loader)
    {
    }

    virtual ~DiskDecoder();

    DiskDecoder(const DiskDecoder&) = delete;
    DiskDecoder(DiskDecoder&&) = delete;
    DiskDecoder& operator=(const DiskDecoder&) = delete;
    DiskDecoder& operator=(DiskDecoder&&) = delete;

    /**
     * @brief Loads a complete graph: graph info, commit log, every commit's
     * skeleton metadata, and full data for the head commit.
     */
    [[nodiscard]] virtual DumpResult<void> decodeGraph(const fs::Path& graphDir) = 0;

    /**
     * @brief Materializes a previously-skeleton commit's full data: graph metadata,
     * journal, tombstones, and every datapart it references (with cross-commit dedup
     * via the loader's @ref GraphLoaderInterface::isDataPartLoaded).
     */
    [[nodiscard]] virtual DumpResult<void> decodeCommitData(const fs::Path& commitDir,
                                                            const fs::Path& partsDir,
                                                            uint64_t commitHash) = 0;

protected:
    GraphLoaderInterface* _loader {nullptr};
};

}
