#pragma once

#include <vector>

#include "DiskDecoder.h"
#include "GraphLoaderInterface.h"

namespace fs {
class FilePageReader;
class FileReader;
class AlignedBufferIterator;
}

namespace db {

class BinaryDiskDecoder : public DiskDecoder {
public:
    explicit BinaryDiskDecoder(GraphLoaderInterface* loader)
        : DiskDecoder(loader)
    {
    }

    ~BinaryDiskDecoder() override;

    [[nodiscard]] DumpResult<void> decodeGraph(const fs::Path& graphDir) override;
    [[nodiscard]] DumpResult<void> decodeCommitData(const fs::Path& commitDir,
                                                    const fs::Path& partsDir,
                                                    uint64_t commitHash) override;

private:
    [[nodiscard]] DumpResult<void> decodeGraphInfo(const fs::Path& graphDir);
    [[nodiscard]] DumpResult<void> decodeCommitLog(const fs::Path& graphDir);
    [[nodiscard]] DumpResult<void> decodeCommitSkeleton(const fs::Path& graphDir,
                                                        uint64_t commitHash);

    [[nodiscard]] DumpResult<void> decodeGraphMetadata(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodeLabelMap(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodeEdgeTypeMap(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodePropertyTypeMap(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodeLabelSetMap(const fs::Path& commitDir);

    [[nodiscard]] DumpResult<void> decodeCommitJournal(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodeTombstones(const fs::Path& commitDir);
    [[nodiscard]] DumpResult<void> decodeCommitMetadataFile(const fs::Path& commitDir,
                                                            uint64_t* outNumNodes,
                                                            uint64_t* outNumEdges,
                                                            uint64_t* outNumCommitDataParts,
                                                            std::vector<uint64_t>* outAllPartIDs);

    [[nodiscard]] DumpResult<void> decodeDataPart(const fs::Path& partsDir, uint64_t partID);

    [[nodiscard]] DumpResult<void> decodeDataPartInfo(const fs::Path& partDir,
                                                      uint64_t* outFirstNodeID,
                                                      uint64_t* outFirstEdgeID);
    [[nodiscard]] DumpResult<void> decodeNodeContainer(const fs::Path& partDir);
    [[nodiscard]] DumpResult<void> decodeEdgeContainer(const fs::Path& partDir);
    [[nodiscard]] DumpResult<void> decodeEdgeIndexer(const fs::Path& partDir);
    [[nodiscard]] DumpResult<void> decodePropertyIndexer(const fs::Path& partDir,
                                                         EntityKind kind,
                                                         std::string_view filename);
    [[nodiscard]] DumpResult<void> decodePropertyContainerFile(const fs::Path& propsFile,
                                                               EntityKind kind,
                                                               uint64_t propTypeID,
                                                               ValueType vt);
    [[nodiscard]] DumpResult<void> decodeStringPropIndexer(const fs::Path& mainFile,
                                                           const fs::Path& auxFile,
                                                           EntityKind kind);

    // Per-value-type property decoders
    [[nodiscard]] DumpResult<void> decodeTrivialPropertyContainer(fs::FilePageReader& reader,
                                                                  EntityKind kind,
                                                                  uint64_t propTypeID,
                                                                  ValueType vt);
    [[nodiscard]] DumpResult<void> decodeStringPropertyContainer(fs::FilePageReader& reader,
                                                                 EntityKind kind,
                                                                 uint64_t propTypeID);
    [[nodiscard]] DumpResult<void> decodeEmbeddingPropertyContainer(fs::FilePageReader& reader,
                                                                    EntityKind kind,
                                                                    uint64_t propTypeID);
};

}
