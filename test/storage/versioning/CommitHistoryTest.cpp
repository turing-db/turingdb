#include <gtest/gtest.h>

#include "ArcManager.h"
#include "DataPart.h"
#include "Graph.h"
#include "ID.h"
#include "comparators/DataPartComparator.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"
#include "TuringTest.h"
#include "versioning/CommitHistory.h"
#include "versioning/Change.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;

class CommitHistoryTest : public TuringTest {
public:
    void initialize() override {
    }

    void terminate() override {
    }
protected:
    CommitHistory _emptyHistory;

};

TEST_F(CommitHistoryTest, nextFromEmpty) {
    // Ensure this history is empty
    ASSERT_EQ(_emptyHistory.commits().size(), 0);
    ASSERT_EQ(_emptyHistory.commitDataparts().size(), 0);
    ASSERT_EQ(_emptyHistory.allDataparts().size(), 0);

    // Create next history entry from the empty history
    CommitHistory newHistory;
    newHistory.newFromPrevious(_emptyHistory);

    // No changes: this should also be empty
    EXPECT_EQ(newHistory.commits().size(), 0);
    EXPECT_EQ(newHistory.commitDataparts().size(), 0);
    EXPECT_EQ(newHistory.allDataparts().size(), 0);    
}

TEST_F(CommitHistoryTest, nextFromNonEmpty) {
    // Empty graph, has one commit by default
    auto graph = Graph::create();
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 1);

    // Make some changes
    GraphWriter w(graph.get());
    w.addNode({LabelID{0}});
    // Submit the changes: creates new commit
    w.submit();

    // Ensure we have a new commit
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 2);
    // Only one change, we should have one datapart
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().back().dataparts().size(), 1);
    auto oldDP = graph->openTransaction().viewGraph().commits().back().dataparts().front();

    // Get the history at this point
    CommitHistory nonEmptyHistory = graph->openTransaction().viewGraph().commits().back().history();
    // Create a new history from this point
    nonEmptyHistory.newFromPrevious(nonEmptyHistory);

    // Ensure that in the new history, we still have one commit
    auto newHistoryLatestCommit = nonEmptyHistory.commits().back();
    EXPECT_EQ(newHistoryLatestCommit.dataparts().size(), 1);
    auto newDP = newHistoryLatestCommit.dataparts().front();

    // Both commits should reference the same datapart: they should point to the same thing
    EXPECT_EQ(oldDP.get(), newDP.get());
}

TEST_F(CommitHistoryTest, concurrentChangesWithRebase) {
     // Empty graph, has one commit by default
    auto graph = Graph::create();
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 1);

    // Both of these create a Change on @ref graph on construction
    GraphWriter delayedWriter(graph.get());
    GraphWriter eagerWriter(graph.get());

    // We should still have only one commit
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 1);

    // Make some changes, but do not commit them
    delayedWriter.addNode({LabelID{0}});
    delayedWriter.addNode({LabelID{1}});
    delayedWriter.addNode({LabelID{2}});

    // Make some changes, and commit them
    eagerWriter.addNode({LabelID{3}});
    eagerWriter.addNode({LabelID{4}});
    eagerWriter.addNode({LabelID{5}});
    eagerWriter.submit();

    // Ensure we have a new commit
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 2);
    // Only one change, we should have one datapart
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().back().dataparts().size(), 1);
    auto history0DP1 =
        graph->openTransaction().viewGraph().commits().back().dataparts().front();

    // Ensure we have the right amount of nodes
    ASSERT_EQ(graph->openTransaction().viewGraph().read().getNodeCount(), 3);

    // Get the history at this point
    CommitHistory history1 =
        graph->openTransaction().viewGraph().commits().back().history();

    // Create a new history from this point
    history1.newFromPrevious(history1);

    // We should have 2 commits: one from the original Graph::create, one from @ref eagerWriter.submit()
    EXPECT_EQ(history1.commits().size(), 1 + 1);

    // Ensure that in the new history, we still have one datapart
    auto history1LatestCommit = history1.commits().back(); // Latest commit
    EXPECT_EQ(history1LatestCommit.dataparts().size(), 1);
    EXPECT_EQ(history1.commitDataparts().size(), 1);      // Same thing, different method
    auto history1DP1 = history1LatestCommit.dataparts().front();

    // Ensure the new history we created references the same datapart as in the previous
    // history
    EXPECT_EQ(history1DP1.get(), history0DP1.get());

    // Submit the other changes
    delayedWriter.submit();

    // We should have 6 nodes total
    ASSERT_EQ(graph->openTransaction().viewGraph().read().getNodeCount(), 6);

    // We should have 3 commits now
    ASSERT_EQ(graph->openTransaction().viewGraph().commits().size(), 1 + 1 + 1);

    // Get a new history at this point
    CommitHistory history2 =
        graph->openTransaction().viewGraph().commits().back().history();
    auto history2DP2 = history2.commitDataparts().back();

    // Create a new history
    history2.newFromPrevious(history2);

    // Newly created history should have:
    // a. 2 dataparts
    // b. Those 2 dataparts should point to the same thing as history1 and history2
    ASSERT_EQ(history2.commitDataparts().size(), 2);
    ASSERT_EQ(history2.commits().back().dataparts().size(), 2);

    EXPECT_EQ(history2.commitDataparts().front().get(), history0DP1.get());
    EXPECT_EQ(history2.commitDataparts().back().get(), history2DP2.get());
}
