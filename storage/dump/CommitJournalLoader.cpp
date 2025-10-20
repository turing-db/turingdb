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
        WriteSet<NodeID>& nodes = journal.nodeWriteSet();
        if (auto res = LoadUtils::loadVector(nodes._set, nodeCount, _reader, it); !res) {
            return res;
        }
    }

    { // Load edge write set
        LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
        const size_t edgeCount = it.get<size_t>();
        WriteSet<EdgeID>& edges = journal.edgeWriteSet();
        if (auto res = LoadUtils::loadVector(edges._set, edgeCount, _reader, it); !res) {
            return res;
        }
    }

    return {};
}
