#include <gtest/gtest.h>

#include "ShellCompletion.h"

using namespace db;

namespace {

constexpr char backspaceKey = 127;

// Apply to the line what typing 'key' with the cursor at '|' produces, and mark the
// new cursor position with '|' again, so a case reads as the prompt would look. A key
// the completion leaves alone falls back to what a plain line editor would do.
std::string typeKey(const ShellCompletion& completion, char key, const std::string& line) {
    const size_t cursor = line.find('|');
    std::string text = line;
    text.erase(cursor, 1);

    ShellCompletion::Edit edit;
    if (!completion.editForKey(key, text, cursor, edit)) {
        if (key == backspaceKey) {
            edit._deleteBefore = 1;
        } else {
            edit._insert.assign(1, key);
            edit._cursor = 1;
        }
    }

    const size_t editAt = cursor - edit._deleteBefore;

    text.erase(editAt, edit._deleteBefore + edit._deleteAfter);
    text.insert(editAt, edit._insert);
    text.insert(editAt + edit._cursor + edit._moveRight, 1, '|');

    return text;
}

std::string firstCompletion(const ShellCompletion& completion, const std::string& prefix) {
    std::vector<ShellCompletion::Candidate> candidates;
    completion.complete(prefix, candidates);

    if (candidates.empty()) {
        return "";
    }

    std::string text = candidates.front()._text;
    text.insert(candidates.front()._cursor, 1, '|');

    return text;
}

}

class ShellCompletionTest : public ::testing::Test {
protected:
    ShellCompletion _completion;
};

TEST_F(ShellCompletionTest, OpenerInsertsItsCloser) {
    EXPECT_EQ(typeKey(_completion, '(', "MATCH |"), "MATCH (|)");
    EXPECT_EQ(typeKey(_completion, '{', "MATCH (n |"), "MATCH (n {|}");
    EXPECT_EQ(typeKey(_completion, '"', "RETURN |"), "RETURN \"|\"");
    EXPECT_EQ(typeKey(_completion, '[', "RETURN |"), "RETURN [|]");
}

TEST_F(ShellCompletionTest, BracketOnDashOpensRelationship) {
    EXPECT_EQ(typeKey(_completion, '[', "MATCH (n)-|"), "MATCH (n)-[|]->");
    EXPECT_EQ(typeKey(_completion, '[', "MATCH (n)<-|"), "MATCH (n)<-[|]-");
}

TEST_F(ShellCompletionTest, CloserStepsOverItself) {
    EXPECT_EQ(typeKey(_completion, ')', "MATCH (n|)"), "MATCH (n)|");
    EXPECT_EQ(typeKey(_completion, '"', "RETURN \"a|\""), "RETURN \"a\"|");
}

TEST_F(ShellCompletionTest, ClosingBracketStepsOverTheWholeArrow) {
    EXPECT_EQ(typeKey(_completion, ']', "MATCH (n)-[:KNOWS|]->"), "MATCH (n)-[:KNOWS]->|");
    EXPECT_EQ(typeKey(_completion, ']', "MATCH (n)<-[:KNOWS|]-"), "MATCH (n)<-[:KNOWS]-|");
    EXPECT_EQ(typeKey(_completion, ']', "RETURN [1,2|]"), "RETURN [1,2]|");
}

TEST_F(ShellCompletionTest, NoPairingInFrontOfText) {
    EXPECT_EQ(typeKey(_completion, '(', "MATCH |abc"), "MATCH (|abc");
}

TEST_F(ShellCompletionTest, BackspaceDeletesBothSides) {
    EXPECT_EQ(typeKey(_completion, backspaceKey, "MATCH (|)"), "MATCH |");
    EXPECT_EQ(typeKey(_completion, backspaceKey, "MATCH (n)-[|]->"), "MATCH (n)-|");
    EXPECT_EQ(typeKey(_completion, backspaceKey, "MATCH (n)|"), "MATCH (n|");
}

TEST_F(ShellCompletionTest, DashCompletesToArrows) {
    EXPECT_EQ(firstCompletion(_completion, "MATCH (a)-"), "MATCH (a)-->|");
    EXPECT_EQ(firstCompletion(_completion, "MATCH (a)<-"), "MATCH (a)<--|");
}

TEST_F(ShellCompletionTest, ArrowCompletionsKeepTheTypedDirection) {
    std::vector<ShellCompletion::Candidate> candidates;
    _completion.complete("MATCH (a)<-", candidates);

    ASSERT_EQ(candidates.size(), 2);
    EXPECT_EQ(candidates[0]._text, "MATCH (a)<--");
    EXPECT_EQ(candidates[1]._text, "MATCH (a)<-[]-");
    EXPECT_EQ(candidates[1]._cursor, std::string("MATCH (a)<-[").size());
}

TEST_F(ShellCompletionTest, WordCompletesToKeyword) {
    EXPECT_EQ(firstCompletion(_completion, "MAT"), "MATCH|");
    EXPECT_EQ(firstCompletion(_completion, "MATCH (n) ret"), "MATCH (n) RETURN|");
}

TEST_F(ShellCompletionTest, FirstWordCompletesToShellCommand) {
    _completion.addCommand("checkout");
    _completion.addCommand("connect");

    EXPECT_EQ(firstCompletion(_completion, "che"), "checkout|");
    EXPECT_EQ(firstCompletion(_completion, "MATCH (n) che"), "");
}

TEST_F(ShellCompletionTest, NothingToCompleteYieldsNoCandidate) {
    std::vector<ShellCompletion::Candidate> candidates;

    _completion.complete("", candidates);
    EXPECT_TRUE(candidates.empty());

    _completion.complete("MATCH (n) ", candidates);
    EXPECT_TRUE(candidates.empty());
}
