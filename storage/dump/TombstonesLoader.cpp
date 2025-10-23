#include "TombstonesLoader.h"

#include "LoadUtils.h"
#include "GraphDumpHelper.h"
#include "versioning/Tombstones.h"

using namespace db;

DumpResult<void> TombstonesLoader::load(Tombstones& tombstones) {
    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_TOMBSTONES,
                                 _reader.error().value());
    }

    // Start reading metadata page
    auto it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    { // Load node tombstones
        LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
        const size_t nodeCount = it.get<size_t>();

        // Load into vector first
        std::vector<NodeID> tempNodeVec;
        tempNodeVec.reserve(nodeCount);
        if (auto res = LoadUtils::loadVector(tempNodeVec, nodeCount, _reader, it); !res) {
            return res;
        }

        // Insert all elements of vector into the tombstones set in one go
        tombstones.edgeTombstones().reserve(nodeCount);
        tombstones.nodeTombstones().insert(tempNodeVec);
    }
    
    { // Load edge tombstones
        LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
        const size_t edgeCount = it.get<size_t>();

        // Load into vector first
        std::vector<EdgeID> tempEdgeVec;
        tempEdgeVec.reserve(edgeCount);
        if (auto res = LoadUtils::loadVector(tempEdgeVec, edgeCount, _reader, it); !res) {
            return res;
        }

        // Insert all elements of vector into the tombstones set in one go
        tombstones.edgeTombstones().reserve(edgeCount);
        tombstones.edgeTombstones().insert(tempEdgeVec);
    }

    return {};
}
