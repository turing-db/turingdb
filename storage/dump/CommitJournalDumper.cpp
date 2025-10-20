#include "CommitJournalDumper.h"

#include "GraphDumpHelper.h"
#include "DumpUtils.h"
#include "versioning/CommitJournal.h"
#include "versioning/WriteSet.h"

using namespace db;

DumpResult<void> CommitJournalDumper::dump(const CommitJournal& journal) {

    GraphDumpHelper::writeFileHeader(_writer);

    { // Dump node write set
        const WriteSet<NodeID>& writtenNodes = journal.nodeWriteSet();
        const size_t nodesSize = writtenNodes.size();

        DumpUtils::ensureDumpSpace(sizeof(nodesSize), _writer);
        _writer.writeToCurrentPage(nodesSize);

        DumpUtils::dumpRange(writtenNodes, _writer);
    }

    { // Dump edge write set
        const WriteSet<EdgeID>& writtenEdges = journal.edgeWriteSet();
        const size_t edgesSize = writtenEdges.size();

        DumpUtils::ensureDumpSpace(sizeof(edgesSize), _writer);
        _writer.writeToCurrentPage(edgesSize);

        DumpUtils::dumpRange(writtenEdges, _writer);
    }

    return {};
}
