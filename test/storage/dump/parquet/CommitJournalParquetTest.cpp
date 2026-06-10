#include "TuringTest.h"

#include "dump/parquet/CommitJournalParquetDumper.h"
#include "dump/parquet/CommitJournalParquetLoader.h"
#include "comparators/WriteSetComparator.h"
#include "versioning/CommitJournal.h"
#include "versioning/WriteSet.h"

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class CommitJournalParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(CommitJournalParquetTest, RoundTrip) {
    const auto original = CommitJournal::emptyJournal();

    original->addWrittenNode(NodeID {5});
    original->addWrittenNode(NodeID {2});
    original->addWrittenNode(NodeID {9});

    original->addWrittenEdge(EdgeID {1});
    original->addWrittenEdge(EdgeID {4});

    // Freeze the journal as a committed one would be — sorts and uniques the write sets.
    original->finalise();

    const fs::Path commitDir = fs::Path(_outDir);
    CommitJournalParquetDumper::dump(*original, commitDir);

    const auto loaded = CommitJournal::emptyJournal();
    CommitJournalParquetLoader::load(commitDir, *loaded);

    EXPECT_TRUE(WriteSetComparator<NodeID>::same(original->nodeWriteSet(), loaded->nodeWriteSet()));
    EXPECT_TRUE(WriteSetComparator<EdgeID>::same(original->edgeWriteSet(), loaded->edgeWriteSet()));
}

TEST_F(CommitJournalParquetTest, EmptyRoundTrip) {
    const auto original = CommitJournal::emptyJournal();

    const fs::Path commitDir = fs::Path(_outDir);
    CommitJournalParquetDumper::dump(*original, commitDir);

    const auto loaded = CommitJournal::emptyJournal();
    CommitJournalParquetLoader::load(commitDir, *loaded);

    EXPECT_TRUE(WriteSetComparator<NodeID>::same(original->nodeWriteSet(), loaded->nodeWriteSet()));
    EXPECT_TRUE(WriteSetComparator<EdgeID>::same(original->edgeWriteSet(), loaded->edgeWriteSet()));
    EXPECT_TRUE(loaded->empty());
}
