#include "BinaryDiskDecoder.h"

#include <vector>

#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "GraphLoaderInterface.h"
#include "LoadUtils.h"
#include "FilePageReader.h"
#include "FileReader.h"
#include "ID.h"
#include "Profiler.h"
#include "StringBucket.h"
#include "metadata/LabelSet.h"

namespace db {

namespace {

constexpr std::string_view NODE_PROPS_PREFIX = "node-props-";
constexpr std::string_view EDGE_PROPS_PREFIX = "edge-props-";
constexpr size_t PROP_PREFIX_SIZE = NODE_PROPS_PREFIX.size();
static_assert(PROP_PREFIX_SIZE == EDGE_PROPS_PREFIX.size());

}

BinaryDiskDecoder::~BinaryDiskDecoder() = default;
DiskDecoder::~DiskDecoder() = default;

// ─────────────────────────────────────────────────────────────────
// Top level
// ─────────────────────────────────────────────────────────────────

DumpResult<void> BinaryDiskDecoder::decodeGraph(const fs::Path& graphDir) {
    Profile profile("BinaryDiskDecoder::decodeGraph");

    if (!graphDir.exists()) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    const auto dirInfo = graphDir.getFileInfo();
    if (!dirInfo) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }
    if (dirInfo->_type != fs::FileType::Directory) {
        return DumpError::result(DumpErrorType::NOT_DIRECTORY);
    }

    if (auto res = decodeGraphInfo(graphDir); !res) {
        return res;
    }

    _loader->initVersionController();

    if (auto res = decodeCommitLog(graphDir); !res) {
        return res;
    }

    // Eagerly load HEAD commit data
    const uint64_t headHash = _loader->getHeadCommitHash();
    const fs::Path commitDir = graphDir / "commits" / fmt::format("{}", headHash);
    const fs::Path partsDir = graphDir / "dataparts";
    return decodeCommitData(commitDir, partsDir, headHash);
}

DumpResult<void> BinaryDiskDecoder::decodeGraphInfo(const fs::Path& graphDir) {
    Profile profile("BinaryDiskDecoder::decodeGraphInfo");

    const fs::Path infoFile = graphDir / "info";
    auto reader = fs::FilePageReader::open(infoFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, reader.error());
    }

    reader.value().nextPage();
    if (reader.value().errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO,
                                 reader.value().error().value());
    }

    auto it = reader.value().begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_GRAPH_INFO);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t graphID = it.get<uint64_t>();
    const uint64_t nameSize = it.get<uint64_t>();
    const std::string_view name = it.get<char>(nameSize);

    _loader->setGraphInfo(graphID, name);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeCommitLog(const fs::Path& graphDir) {
    Profile profile("BinaryDiskDecoder::decodeCommitLog");

    const fs::Path logFile = graphDir / "commitlog";
    auto fileRes = fs::File::open(logFile);
    if (!fileRes) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_COMMIT_LOG, fileRes.error());
    }

    fs::FileReader reader;
    reader.setFile(&fileRes.value());
    reader.read();
    if (reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_COMMIT_LOG);
    }

    fs::ByteBufferIterator it = reader.iterateBuffer();
    if (auto res = GraphDumpHelper::checkFileHeader(&it); !res) {
        return res.get_unexpected();
    }

    const uint64_t numEntries = it.get<uint64_t>();
    _loader->setNumCommits(numEntries);
    _loader->reserveCommits(numEntries);

    for (size_t i = 0; i < numEntries; i++) {
        const uint64_t hash = it.get<uint64_t>();
        if (auto res = decodeCommitSkeleton(graphDir, hash); !res) {
            return res;
        }
    }

    if (numEntries == 0) {
        return DumpError::result(DumpErrorType::NO_COMMITS);
    }

    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeCommitSkeleton(const fs::Path& graphDir,
                                                         uint64_t commitHash) {
    Profile profile("BinaryDiskDecoder::decodeCommitSkeleton");

    const fs::Path commitDir = graphDir / "commits" / fmt::format("{}", commitHash);

    uint64_t numNodes = 0;
    uint64_t numEdges = 0;
    uint64_t numCommitDataParts = 0;
    if (auto res = decodeCommitMetadataFile(commitDir, &numNodes, &numEdges,
                                            &numCommitDataParts, nullptr);
        !res) {
        return res;
    }

    _loader->addCommitSkeleton(commitHash, numNodes, numEdges, numCommitDataParts);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeCommitData(const fs::Path& commitDir,
                                                     const fs::Path& partsDir,
                                                     uint64_t commitHash) {
    Profile profile("BinaryDiskDecoder::decodeCommitData");

    _loader->beginCommitData(commitHash);

    if (auto res = decodeGraphMetadata(commitDir); !res) {
        _loader->endCommitData();
        return res;
    }
    if (auto res = decodeCommitJournal(commitDir); !res) {
        _loader->endCommitData();
        return res;
    }
    if (auto res = decodeTombstones(commitDir); !res) {
        _loader->endCommitData();
        return res;
    }

    uint64_t numNodes = 0, numEdges = 0, numCommitDataParts = 0;
    std::vector<uint64_t> allPartIDs;
    if (auto res = decodeCommitMetadataFile(commitDir, &numNodes, &numEdges,
                                            &numCommitDataParts, &allPartIDs);
        !res) {
        _loader->endCommitData();
        return res;
    }

    for (uint64_t partID : allPartIDs) {
        if (_loader->isDataPartLoaded(partID)) {
            _loader->attachExistingDataPart(partID);
            continue;
        }
        if (auto res = decodeDataPart(partsDir, partID); !res) {
            _loader->endCommitData();
            return res;
        }
    }

    _loader->setCommitDataPartCount(numCommitDataParts);
    _loader->endCommitData();
    return {};
}

// ─────────────────────────────────────────────────────────────────
// Graph metadata (commit dir)
// ─────────────────────────────────────────────────────────────────

DumpResult<void> BinaryDiskDecoder::decodeGraphMetadata(const fs::Path& commitDir) {
    if (auto labels = decodeLabelMap(commitDir); !labels) {
        return labels;
    } else if (auto edgeTypes = decodeEdgeTypeMap(commitDir); !edgeTypes) {
        return edgeTypes;
    } else if (auto propTypes = decodePropertyTypeMap(commitDir); !propTypes) {
        return propTypes;
    } else if (auto labelsets = decodeLabelSetMap(commitDir); !labelsets) {
        return labelsets;
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeLabelMap(const fs::Path& commitDir) {
    const fs::Path labelsFile = commitDir / "labels";
    auto reader = fs::FilePageReader::open(labelsFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS, rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t labelCount = it.get<uint64_t>();
    const uint64_t pageCount = it.get<uint64_t>();

    uint64_t added = 0;
    for (size_t i = 0; i < pageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS, rd.error().value());
        }
        it = rd.begin();
        if (it.remainingBytes() < DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
        }
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const uint64_t strsize = it.get<uint64_t>();
            const std::string_view name = it.get<char>(strsize);
            _loader->addLabel(name);
            added++;
        }
    }
    if (added != labelCount) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELS);
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeEdgeTypeMap(const fs::Path& commitDir) {
    const fs::Path edgeTypesFile = commitDir / "edge-types";
    auto reader = fs::FilePageReader::open(edgeTypesFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES, rd.error().value());
    }
    auto it = rd.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t edgeTypeCount = it.get<uint64_t>();
    const uint64_t pageCount = it.get<uint64_t>();

    uint64_t added = 0;
    for (size_t i = 0; i < pageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const uint64_t strsize = it.get<uint64_t>();
            const std::string_view name = it.get<char>(strsize);
            _loader->addEdgeType(name);
            added++;
        }
    }
    if (added != edgeTypeCount) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_EDGE_TYPES);
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodePropertyTypeMap(const fs::Path& commitDir) {
    const fs::Path file = commitDir / "property-types";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES, rd.error().value());
    }
    auto it = rd.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t propTypeCount = it.get<uint64_t>();
    const uint64_t pageCount = it.get<uint64_t>();

    uint64_t added = 0;
    for (size_t i = 0; i < pageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const ValueType vt = it.get<ValueType>();
            const uint64_t strsize = it.get<uint64_t>();
            const std::string_view name = it.get<char>(strsize);
            _loader->addPropertyType(name, vt);
            added++;
        }
    }
    if (added != propTypeCount) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_TYPES);
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeLabelSetMap(const fs::Path& commitDir) {
    const fs::Path file = commitDir / "labelsets";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS, rd.error().value());
    }
    auto it = rd.begin();
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

    uint64_t added = 0;
    std::vector<uint64_t> bits(LabelSet::IntegerCount);

    for (size_t i = 0; i < pageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            for (size_t k = 0; k < LabelSet::IntegerCount; k++) {
                bits[k] = it.get<LabelSet::IntegerType>();
            }
            _loader->addLabelSet(bits.data(), bits.size());
            added++;
        }
    }
    if (added != labelsetsCount) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS);
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeCommitJournal(const fs::Path& commitDir) {
    const fs::Path file = commitDir / "journal";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_JOURNAL, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_JOURNAL, rd.error().value());
    }
    auto it = rd.begin();
    LoadUtils::ensureIteratorReadPage(it);
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    {
        LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
        const size_t nodeCount = it.get<size_t>();
        std::vector<NodeID> nodes;
        if (auto res = LoadUtils::loadVector(nodes, nodeCount, rd, it); !res) {
            return res;
        }
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        for (auto n : nodes) ids.push_back(n.getValue());
        _loader->addNodeWriteSet(ids.data(), ids.size());
    }
    {
        LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
        const size_t edgeCount = it.get<size_t>();
        std::vector<EdgeID> edges;
        if (auto res = LoadUtils::loadVector(edges, edgeCount, rd, it); !res) {
            return res;
        }
        std::vector<uint64_t> ids;
        ids.reserve(edges.size());
        for (auto e : edges) ids.push_back(e.getValue());
        _loader->addEdgeWriteSet(ids.data(), ids.size());
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeTombstones(const fs::Path& commitDir) {
    const fs::Path file = commitDir / "tombstones";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_TOMBSTONES, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_TOMBSTONES, rd.error().value());
    }
    auto it = rd.begin();
    LoadUtils::ensureIteratorReadPage(it);
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    {
        LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
        const size_t nodeCount = it.get<size_t>();
        std::vector<NodeID> nodes;
        if (auto res = LoadUtils::loadVector(nodes, nodeCount, rd, it); !res) {
            return res;
        }
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        for (auto n : nodes) ids.push_back(n.getValue());
        _loader->addNodeTombstones(ids.data(), ids.size());
    }
    {
        LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
        const size_t edgeCount = it.get<size_t>();
        std::vector<EdgeID> edges;
        if (auto res = LoadUtils::loadVector(edges, edgeCount, rd, it); !res) {
            return res;
        }
        std::vector<uint64_t> ids;
        ids.reserve(edges.size());
        for (auto e : edges) ids.push_back(e.getValue());
        _loader->addEdgeTombstones(ids.data(), ids.size());
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeCommitMetadataFile(
    const fs::Path& commitDir,
    uint64_t* outNumNodes,
    uint64_t* outNumEdges,
    uint64_t* outNumCommitDataParts,
    std::vector<uint64_t>* outAllPartIDs) {
    const fs::Path file = commitDir / "metadata";
    auto fileRes = fs::File::open(file);
    if (!fileRes) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_COMMIT_METADATA, fileRes.error());
    }
    fs::FileReader reader;
    reader.setFile(&fileRes.value());
    reader.read();
    if (reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_COMMIT_METADATA);
    }

    fs::ByteBufferIterator it = reader.iterateBuffer();
    if (auto res = GraphDumpHelper::checkFileHeader(&it); !res) {
        return res.get_unexpected();
    }

    *outNumNodes = it.get<uint64_t>();
    *outNumEdges = it.get<uint64_t>();
    *outNumCommitDataParts = it.get<uint64_t>();

    const uint64_t numAll = it.get<uint64_t>();
    if (outAllPartIDs) {
        outAllPartIDs->reserve(numAll);
        for (uint64_t i = 0; i < numAll; i++) {
            outAllPartIDs->push_back(it.get<uint64_t>());
        }
    }
    return {};
}

// ─────────────────────────────────────────────────────────────────
// DataPart
// ─────────────────────────────────────────────────────────────────

DumpResult<void> BinaryDiskDecoder::decodeDataPart(const fs::Path& partsDir, uint64_t partID) {
    Profile profile("BinaryDiskDecoder::decodeDataPart");

    const fs::Path partDir = partsDir / fmt::format("{}", partID);
    if (!partDir.exists()) {
        return DumpError::result(DumpErrorType::DATAPART_DOES_NOT_EXIST);
    }

    uint64_t firstNodeID = 0;
    uint64_t firstEdgeID = 0;
    if (auto res = decodeDataPartInfo(partDir, &firstNodeID, &firstEdgeID); !res) {
        return res;
    }

    _loader->beginDataPart(partID, firstNodeID, firstEdgeID);

    // Nodes (optional)
    const fs::Path nodesFile = partDir / "nodes";
    if (nodesFile.exists()) {
        if (auto res = decodeNodeContainer(partDir); !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    } else {
        _loader->initEmptyNodeContainer();
    }

    // Edges (optional)
    const fs::Path edgesFile = partDir / "edges";
    if (edgesFile.exists()) {
        if (auto res = decodeEdgeContainer(partDir); !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    } else {
        _loader->initEmptyEdgeContainer();
    }

    // Edge indexer (optional)
    const fs::Path edgeIndexerFile = partDir / "edge-indexer";
    if (edgeIndexerFile.exists()) {
        if (auto res = decodeEdgeIndexer(partDir); !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }

    // Node + edge property indexers (optional)
    const fs::Path nodePropIndexerFile = partDir / "node-prop-indexer";
    if (nodePropIndexerFile.exists()) {
        if (auto res = decodePropertyIndexer(partDir, EntityKind::Node, "node-prop-indexer");
            !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }
    const fs::Path edgePropIndexerFile = partDir / "edge-prop-indexer";
    if (edgePropIndexerFile.exists()) {
        if (auto res = decodePropertyIndexer(partDir, EntityKind::Edge, "edge-prop-indexer");
            !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }

    // Property containers — dispatched by filename suffix
    auto files = partDir.listDir();
    if (!files) {
        return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPART_FILES, files.error());
    }

    for (const auto& child : files.value()) {
        const std::string_view childStr = child.filename();
        EntityKind kind;
        if (childStr.find(NODE_PROPS_PREFIX) == 0) {
            kind = EntityKind::Node;
        } else if (childStr.find(EDGE_PROPS_PREFIX) == 0) {
            kind = EntityKind::Edge;
        } else {
            continue;
        }

        const auto ptIDRes = GraphDumpHelper::getIntegerSuffix(childStr, PROP_PREFIX_SIZE);
        if (!ptIDRes) {
            return DumpError::result(DumpErrorType::INCORRECT_PROPERTY_TYPE_ID);
        }

        // Read just the metadata page to learn the value type, then dispatch
        const fs::Path propsFile = partDir / childStr;
        auto reader = fs::FilePageReader::open(propsFile, DumpConfig::PAGE_SIZE);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS,
                                     reader.error());
        }
        reader.value().nextPage();
        if (reader.value().errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                     reader.value().error().value());
        }
        auto it = reader.value().begin();
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }
        const ValueType vt = it.get<ValueType>();

        // Re-open from the start so the inner decoder consumes the metadata page itself.
        auto reader2 = fs::FilePageReader::open(propsFile, DumpConfig::PAGE_SIZE);
        if (!reader2) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS,
                                     reader2.error());
        }
        if (auto res = decodePropertyContainerFile(propsFile, kind, ptIDRes.value(), vt);
            !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }

#ifndef DISABLE_STRING_INDEX
    // String prop indexers (node + edge)
    {
        const fs::Path main = partDir / "node-string-prop-indexer";
        const fs::Path aux = partDir / "node-string-prop-indexer-owners";
        if (!main.exists() || !aux.exists()) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return DumpError::result(
                DumpErrorType::CANNOT_OPEN_DATAPART_NODE_STR_PROP_INDEXER);
        }
        if (auto res = decodeStringPropIndexer(main, aux, EntityKind::Node); !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }
    {
        const fs::Path main = partDir / "edge-string-prop-indexer";
        const fs::Path aux = partDir / "edge-string-prop-indexer-owners";
        if (!main.exists() || !aux.exists()) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return DumpError::result(
                DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_STR_PROP_INDEXER);
        }
        if (auto res = decodeStringPropIndexer(main, aux, EntityKind::Edge); !res) {
            _loader->finalizeDataPart();
            _loader->endDataPart();
            return res;
        }
    }
#endif

    _loader->finalizeDataPart();
    _loader->endDataPart();
    _loader->attachDataPartToCommit(partID);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeDataPartInfo(const fs::Path& partDir,
                                                       uint64_t* outFirstNodeID,
                                                       uint64_t* outFirstEdgeID) {
    const fs::Path infoFile = partDir / "info";
    auto reader = fs::FilePageReader::open(infoFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_INFO, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_DATAPART_INFO,
                                 rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_DATAPART_INFO);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    // nodeCount, edgeCount written but unused at load time (recovered from container files)
    [[maybe_unused]] const uint64_t nodeCount = it.get<uint64_t>();
    [[maybe_unused]] const uint64_t edgeCount = it.get<uint64_t>();
    *outFirstNodeID = it.get<uint64_t>();
    *outFirstEdgeID = it.get<uint64_t>();
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeNodeContainer(const fs::Path& partDir) {
    const fs::Path file = partDir / "nodes";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODES, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t firstID = it.get<uint64_t>();
    const uint64_t nodeCount = it.get<uint64_t>();
    [[maybe_unused]] const uint64_t rangeCount = it.get<uint64_t>();
    const uint64_t recordPageCount = it.get<uint64_t>();
    const uint64_t rangePageCount = it.get<uint64_t>();

    _loader->beginNodeContainer(firstID, nodeCount);

    // Range pages
    for (size_t i = 0; i < rangePageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, rd.error().value());
        }
        it = rd.begin();
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
        }
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const uint64_t lsetID = it.get<LabelSetID::Type>();
            const uint64_t rangeFirst = it.get<uint64_t>();
            const uint64_t rangeCnt = it.get<uint64_t>();
            _loader->addNodeRange(lsetID, rangeFirst, rangeCnt);
        }
    }

    // Record pages
    std::vector<uint64_t> labelsetIDs;
    for (size_t i = 0; i < recordPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, rd.error().value());
        }
        it = rd.begin();
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
        }
        const size_t countInPage = it.get<uint64_t>();
        labelsetIDs.clear();
        labelsetIDs.reserve(countInPage);
        for (size_t j = 0; j < countInPage; j++) {
            labelsetIDs.push_back(it.get<LabelSetID::Type>());
        }
        _loader->appendNodeRecords(labelsetIDs.data(), labelsetIDs.size());
    }

    _loader->endNodeContainer();
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeEdgeContainer(const fs::Path& partDir) {
    const fs::Path file = partDir / "edges";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGES, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES, rd.error().value());
    }
    auto it = rd.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t firstEdgeID = it.get<uint64_t>();
    const uint64_t firstNodeID = it.get<uint64_t>();
    const uint64_t edgeCount = it.get<uint64_t>();
    const uint64_t pageCountPerDir = it.get<uint64_t>();

    _loader->beginEdgeContainer(firstNodeID, firstEdgeID, edgeCount);

    const auto loadOneDirection = [&](EdgeDirection dir) -> DumpResult<void> {
        std::vector<EdgeRecordRaw> records;
        for (size_t i = 0; i < pageCountPerDir; i++) {
            rd.nextPage();
            if (rd.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES,
                                         rd.error().value());
            }
            it = rd.begin();
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES);
            }
            const size_t countInPage = it.get<uint64_t>();
            records.clear();
            records.reserve(countInPage);
            for (size_t j = 0; j < countInPage; j++) {
                EdgeRecordRaw r;
                r._edgeID = it.get<uint64_t>();
                r._nodeID = it.get<uint64_t>();
                r._otherID = it.get<uint64_t>();
                r._edgeTypeID = it.get<uint64_t>();
                records.push_back(r);
            }
            _loader->appendEdgeRecords(dir, records.data(), records.size());
        }
        return {};
    };

    if (auto outRes = loadOneDirection(EdgeDirection::Out); !outRes) {
        return outRes;
    } else if (auto inRes = loadOneDirection(EdgeDirection::In); !inRes) {
        return inRes;
    }

    _loader->endEdgeContainer();
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeEdgeIndexer(const fs::Path& partDir) {
    const fs::Path file = partDir / "edge-indexer";
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_INDEXER,
                                 reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                 rd.error().value());
    }
    auto it = rd.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    const uint64_t firstNodeID = it.get<uint64_t>();
    const uint64_t firstEdgeID = it.get<uint64_t>();
    const uint64_t coreNodeCount = it.get<uint64_t>();
    const uint64_t patchNodeCount = it.get<uint64_t>();
    const uint64_t nodePageCount = it.get<uint64_t>();
    const uint64_t outSpansPageCount = it.get<uint64_t>();
    const uint64_t inSpansPageCount = it.get<uint64_t>();

    _loader->beginEdgeIndexer(firstNodeID, firstEdgeID, coreNodeCount, patchNodeCount);

    // Node-edge ranges
    std::vector<NodeEdgeRangesRaw> ranges;
    for (size_t i = 0; i < nodePageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                     rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        ranges.clear();
        ranges.reserve(countInPage);
        for (size_t j = 0; j < countInPage; j++) {
            NodeEdgeRangesRaw r;
            r._outFirst = it.get<size_t>();
            r._outCount = it.get<size_t>();
            r._inFirst = it.get<size_t>();
            r._inCount = it.get<size_t>();
            ranges.push_back(r);
        }
        _loader->appendNodeEdgeRanges(ranges.data(), ranges.size());
    }

    _loader->finalizeEdgeIndexerPatchNodes();

    const auto loadSpans = [&](EdgeDirection dir, uint64_t pageCount) -> DumpResult<void> {
        for (size_t i = 0; i < pageCount; i++) {
            rd.nextPage();
            if (rd.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGE_INDEXER,
                                         rd.error().value());
            }
            it = rd.begin();
            const size_t indexerCountInPage = it.get<uint64_t>();
            for (size_t j = 0; j < indexerCountInPage; j++) {
                const uint64_t spanCount = it.get<uint64_t>();
                const uint64_t labelsetID = it.get<LabelSetID::Type>();
                for (size_t k = 0; k < spanCount; k++) {
                    const uint64_t offset = it.get<uint64_t>();
                    const uint64_t count = it.get<uint64_t>();
                    _loader->appendEdgeIndexerSpan(dir, labelsetID, offset, count);
                }
            }
        }
        return {};
    };

    if (auto outRes = loadSpans(EdgeDirection::Out, outSpansPageCount); !outRes) {
        return outRes;
    } else if (auto inRes = loadSpans(EdgeDirection::In, inSpansPageCount); !inRes) {
        return inRes;
    }

    _loader->endEdgeIndexer();
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodePropertyIndexer(const fs::Path& partDir,
                                                          EntityKind kind,
                                                          std::string_view filename) {
    const fs::Path file = partDir / std::string {filename};
    auto reader = fs::FilePageReader::open(file, DumpConfig::PAGE_SIZE);
    if (!reader) {
        const auto err = (kind == EntityKind::Node)
                             ? DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROP_INDEXER
                             : DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_PROP_INDEXER;
        return DumpError::result(err, reader.error());
    }
    auto& rd = reader.value();

    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER,
                                 rd.error().value());
    }
    auto it = rd.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    [[maybe_unused]] const uint64_t propTypeCount = it.get<uint64_t>();
    const uint64_t pageCount = it.get<uint64_t>();

    std::vector<PropIndexRangeRaw> ranges;
    for (size_t i = 0; i < pageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROP_INDEXER,
                                     rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const uint64_t ptID = it.get<PropertyTypeID::Type>();
            const uint64_t lsetCount = it.get<uint64_t>();
            for (size_t k = 0; k < lsetCount; k++) {
                const uint64_t lsetID = it.get<LabelSetID::Type>();
                const uint64_t rngCount = it.get<uint64_t>();
                ranges.clear();
                ranges.reserve(rngCount);
                for (size_t r = 0; r < rngCount; r++) {
                    PropIndexRangeRaw rr;
                    rr._offset = it.get<uint64_t>();
                    rr._count = it.get<uint64_t>();
                    ranges.push_back(rr);
                }
                _loader->appendPropertyIndexerEntry(kind, ptID, lsetID,
                                                    ranges.data(), ranges.size());
            }
        }
    }
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodePropertyContainerFile(const fs::Path& propsFile,
                                                                EntityKind kind,
                                                                uint64_t propTypeID,
                                                                ValueType vt) {
    auto reader = fs::FilePageReader::open(propsFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_DATAPART_NODE_PROPS,
                                 reader.error());
    }

    switch (vt) {
        case ValueType::UInt64:
        case ValueType::Int64:
        case ValueType::Double:
        case ValueType::Bool:
            return decodeTrivialPropertyContainer(reader.value(), kind, propTypeID, vt);
        case ValueType::String:
            return decodeStringPropertyContainer(reader.value(), kind, propTypeID);
        case ValueType::Embedding:
            return decodeEmbeddingPropertyContainer(reader.value(), kind, propTypeID);
        case ValueType::Invalid:
        case ValueType::_SIZE:
            return DumpError::result(DumpErrorType::INCORRECT_PROPERTY_TYPE_ID);
    }
    return DumpError::result(DumpErrorType::INCORRECT_PROPERTY_TYPE_ID);
}

DumpResult<void> BinaryDiskDecoder::decodeTrivialPropertyContainer(fs::FilePageReader& rd,
                                                                   EntityKind kind,
                                                                   uint64_t propTypeID,
                                                                   ValueType vt) {
    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    [[maybe_unused]] const ValueType vtRead = it.get<ValueType>();
    const uint64_t propCount = it.get<uint64_t>();
    const uint64_t idPageCount = it.get<uint64_t>();
    const uint64_t valuePageCount = it.get<uint64_t>();

    _loader->registerPropertyContainer(kind, propTypeID, vt, propCount, /*dim*/ 0);

    // IDs
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < idPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        ids.clear();
        ids.reserve(countInPage);
        for (size_t j = 0; j < countInPage; j++) {
            ids.push_back(it.get<EntityID::Type>());
        }
        _loader->appendPropertyIDs(kind, propTypeID, ids.data(), ids.size());
    }

    // Values
    for (size_t i = 0; i < valuePageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        switch (vt) {
            case ValueType::UInt64: {
                std::vector<uint64_t> vs(countInPage);
                for (size_t j = 0; j < countInPage; j++) vs[j] = it.get<uint64_t>();
                _loader->appendUInt64PropertyValues(kind, propTypeID, vs.data(), vs.size());
                break;
            }
            case ValueType::Int64: {
                std::vector<int64_t> vs(countInPage);
                for (size_t j = 0; j < countInPage; j++) vs[j] = it.get<int64_t>();
                _loader->appendInt64PropertyValues(kind, propTypeID, vs.data(), vs.size());
                break;
            }
            case ValueType::Double: {
                std::vector<double> vs(countInPage);
                for (size_t j = 0; j < countInPage; j++) vs[j] = it.get<double>();
                _loader->appendDoublePropertyValues(kind, propTypeID, vs.data(), vs.size());
                break;
            }
            case ValueType::Bool: {
                std::vector<uint8_t> vs(countInPage);
                for (size_t j = 0; j < countInPage; j++) vs[j] = it.get<uint8_t>();
                _loader->appendBoolPropertyValues(kind, propTypeID, vs.data(), vs.size());
                break;
            }
            default:
                return DumpError::result(DumpErrorType::INCORRECT_PROPERTY_TYPE_ID);
        }
    }

    _loader->finalizePropertyContainer(kind, propTypeID);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeStringPropertyContainer(fs::FilePageReader& rd,
                                                                  EntityKind kind,
                                                                  uint64_t propTypeID) {
    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    [[maybe_unused]] const ValueType vtRead = it.get<ValueType>();
    const uint64_t propCount = it.get<uint64_t>();
    const uint64_t idPageCount = it.get<uint64_t>();
    const uint64_t bucketPageCount = it.get<uint64_t>();
    const uint64_t limitsPageCount = it.get<uint64_t>();

    _loader->registerPropertyContainer(kind, propTypeID, ValueType::String, propCount, 0);

    // IDs
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < idPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        ids.clear();
        ids.reserve(countInPage);
        for (size_t j = 0; j < countInPage; j++) {
            ids.push_back(it.get<EntityID::Type>());
        }
        _loader->appendPropertyIDs(kind, propTypeID, ids.data(), ids.size());
    }

    // Buckets — accumulate raw bytes
    std::vector<std::vector<char>> rawBuckets;
    rawBuckets.reserve(bucketPageCount * 2);
    for (size_t i = 0; i < bucketPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const std::span chars = it.get<char>(StringBucket::BUCKET_SIZE);
            std::vector<char> bucket(chars.begin(), chars.end());
            rawBuckets.push_back(std::move(bucket));
        }
    }

    // Limits — gives (offset, count) pairs grouped by bucketIndex; reconstruct strings
    // and stream them out as values in encounter order.
    for (size_t i = 0; i < limitsPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t blockCountInPage = it.get<uint64_t>();
        for (size_t j = 0; j < blockCountInPage; j++) {
            const uint64_t bucketIndex = it.get<uint64_t>();
            const uint32_t blockStrCount = it.get<uint32_t>();
            for (size_t k = 0; k < blockStrCount; k++) {
                const uint32_t off = it.get<uint32_t>();
                const uint32_t cnt = it.get<uint32_t>();
                const std::string_view sv {rawBuckets[bucketIndex].data() + off, cnt};
                _loader->appendStringPropertyValue(kind, propTypeID, sv);
            }
        }
    }

    _loader->finalizePropertyContainer(kind, propTypeID);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeEmbeddingPropertyContainer(fs::FilePageReader& rd,
                                                                     EntityKind kind,
                                                                     uint64_t propTypeID) {
    rd.nextPage();
    if (rd.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
    }
    auto it = rd.begin();
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    [[maybe_unused]] const ValueType vtRead = it.get<ValueType>();
    const uint64_t propCount = it.get<uint64_t>();
    const uint64_t dimension = it.get<uint64_t>();
    const uint64_t idPageCount = it.get<uint64_t>();
    const uint64_t floatPageCount = it.get<uint64_t>();

    _loader->registerPropertyContainer(kind, propTypeID, ValueType::Embedding,
                                       propCount, dimension);

    std::vector<uint64_t> ids;
    for (size_t i = 0; i < idPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        ids.clear();
        ids.reserve(countInPage);
        for (size_t j = 0; j < countInPage; j++) {
            ids.push_back(it.get<EntityID::Type>());
        }
        _loader->appendPropertyIDs(kind, propTypeID, ids.data(), ids.size());
    }

    // Read all floats flat, then ship in chunks of `dimension`.
    std::vector<float> buffer;
    buffer.reserve(dimension);

    for (size_t i = 0; i < floatPageCount; i++) {
        rd.nextPage();
        if (rd.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, rd.error().value());
        }
        it = rd.begin();
        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            buffer.push_back(it.get<float>());
            if (buffer.size() == dimension) {
                _loader->appendEmbeddingPropertyValues(kind, propTypeID, buffer.data(),
                                                       1, dimension);
                buffer.clear();
            }
        }
    }

    _loader->finalizePropertyContainer(kind, propTypeID);
    return {};
}

DumpResult<void> BinaryDiskDecoder::decodeStringPropIndexer(const fs::Path& mainFile,
                                                            const fs::Path& auxFile,
                                                            EntityKind kind) {
    auto reader = fs::FilePageReader::open(mainFile, DumpConfig::PAGE_SIZE);
    auto auxReader = fs::FilePageReader::open(auxFile, DumpConfig::PAGE_SIZE);
    if (!reader) {
        const auto err = (kind == EntityKind::Node)
                             ? DumpErrorType::CANNOT_OPEN_DATAPART_NODE_STR_PROP_INDEXER
                             : DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_STR_PROP_INDEXER;
        return DumpError::result(err, reader.error());
    }
    if (!auxReader) {
        const auto err = (kind == EntityKind::Node)
                             ? DumpErrorType::CANNOT_OPEN_DATAPART_NODE_STR_PROP_INDEXER
                             : DumpErrorType::CANNOT_OPEN_DATAPART_EDGE_STR_PROP_INDEXER;
        return DumpError::result(err, auxReader.error());
    }
    auto& rd = reader.value();
    auto& aux = auxReader.value();

    rd.nextPage();
    aux.nextPage();
    auto it = rd.begin();
    LoadUtils::ensureIteratorReadPage(it);
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
    const size_t numIndexes = it.get<size_t>();
    rd.nextPage();

    if (numIndexes == 0) {
        _loader->finalizeStringPropIndexer(kind);
        return {};
    }

    it = rd.begin();
    LoadUtils::ensureIteratorReadPage(it);
    auto auxIt = aux.begin();
    LoadUtils::ensureIteratorReadPage(auxIt);

    for (size_t i = 0; i < numIndexes; i++) {
        LoadUtils::ensureLoadSpace(sizeof(PropertyTypeID::Type), rd, it);
        const uint64_t propID = it.get<PropertyTypeID::Type>();

        LoadUtils::ensureLoadSpace(sizeof(size_t), rd, it);
        const size_t numTreeNodes = it.get<size_t>();

        _loader->beginStringPropIndex(kind, propID, numTreeNodes);

        for (size_t n = 0; n < numTreeNodes; n++) {
            // Each tree-node block bounded by StringIndexDumpConstants::MAXNODESIZE
            // Refresh page if too small
            LoadUtils::ensureLoadSpace(/*MAX-ish:*/ 4096, rd, it);

            const size_t nodeID = it.get<size_t>();
            const size_t numChildren = it.get<size_t>();
            for (size_t c = 0; c < numChildren; c++) {
                const size_t childIndex = it.get<size_t>();
                const size_t childID = it.get<size_t>();
                _loader->setStringPropIndexChild(kind, propID, nodeID, childIndex, childID);
            }
            const size_t numOwners = it.get<size_t>();

            if (numOwners > 0) {
                std::vector<EntityID> owners;
                if (auto res = LoadUtils::loadVector(owners, numOwners, aux, auxIt); !res) {
                    return res;
                }
                std::vector<uint64_t> ids;
                ids.reserve(owners.size());
                for (auto o : owners) ids.push_back(o.getValue());
                _loader->appendStringPropIndexOwners(kind, propID, nodeID,
                                                     ids.data(), ids.size());
            }
        }

        _loader->endStringPropIndex(kind, propID);
    }

    _loader->finalizeStringPropIndexer(kind);
    return {};
}

}
