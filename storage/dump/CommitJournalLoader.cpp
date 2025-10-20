#include "CommitJournalLoader.h"

#include "LoadUtils.h"
#include "GraphDumpHelper.h"
#include "versioning/CommitJournal.h"


using namespace db;

DumpResult<void> CommitJournalLoader::load(CommitJournal& journal) {
    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_LABELSETS,
                                 _reader.error().value());
    }

    // Start reading metadata page
    auto it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    { // Load node write set
        LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
        const size_t nodeCount = it.get<size_t>();
        std::vector<NodeID>& nodes = journal.rawNodeWriteSet();
        if (auto res = LoadUtils::loadVector(nodes, nodeCount, _reader, it); !res) {
            return res;
        }
    }

    { // Load edge write set
        LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
        const size_t edgeCount = it.get<size_t>();
        std::vector<EdgeID>& edges = journal.rawEdgeWriteSet();
        if (auto res = LoadUtils::loadVector(edges, edgeCount, _reader, it); !res) {
            return res;
        }
    }

    return {};
}
