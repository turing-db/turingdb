#include <gtest/gtest.h>

#include <stdlib.h>
#include <string>

#include "TuringClient.h"
#include "LocalMemory.h"
#include "TuringException.h"

using namespace db;

namespace {

// The setters under test only mutate local fields, so a TuringClient can be
// constructed without ever connecting to a server.
class TuringClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // The constructor seeds the token from TURINGDB_AUTH_TOKEN when set;
        // clear it so construction is deterministic regardless of the runner's
        // environment.
        ::unsetenv("TURINGDB_AUTH_TOKEN");
    }

    db::LocalMemory _localMem;
    net::proto::TuringClient _client {"127.0.0.1", "6666", &_localMem};
};

}

// -------------------------------------------------------------------
// setAuthToken
// -------------------------------------------------------------------

TEST_F(TuringClientTest, AcceptsWellFormedAuthToken) {
    EXPECT_NO_THROW(_client.setAuthToken("sk-abc123.DEF_456-789/xyz+=="));
}

TEST_F(TuringClientTest, AcceptsEmptyAuthToken) {
    EXPECT_NO_THROW(_client.setAuthToken(""));
}

TEST_F(TuringClientTest, RejectsAuthTokenWithCarriageReturn) {
    EXPECT_THROW(_client.setAuthToken("abc\rdef"), TuringException);
}

TEST_F(TuringClientTest, RejectsAuthTokenWithLineFeed) {
    EXPECT_THROW(_client.setAuthToken("abc\ndef"), TuringException);
}

TEST_F(TuringClientTest, RejectsAuthTokenWithCrlfHeaderInjection) {
    // The motivating attack: terminate the Authorization header and splice in
    // an extra one.
    EXPECT_THROW(_client.setAuthToken("abc\r\nX-Injected: evil"), TuringException);
}

TEST_F(TuringClientTest, RejectsAuthTokenWithNul) {
    const std::string token("abc\0def", 7);
    EXPECT_THROW(_client.setAuthToken(token), TuringException);
}

TEST_F(TuringClientTest, RejectsAuthTokenWithTab) {
    EXPECT_THROW(_client.setAuthToken("abc\tdef"), TuringException);
}

// -------------------------------------------------------------------
// setGraphName
// -------------------------------------------------------------------

TEST_F(TuringClientTest, AcceptsWellFormedGraphName) {
    EXPECT_NO_THROW(_client.setGraphName("my_graph-2"));
    EXPECT_EQ(_client.getGraphName(), "my_graph-2");
}

TEST_F(TuringClientTest, RejectsGraphNameWithCarriageReturn) {
    EXPECT_THROW(_client.setGraphName("graph\rname"), TuringException);
}

TEST_F(TuringClientTest, RejectsGraphNameWithLineFeed) {
    EXPECT_THROW(_client.setGraphName("graph\nname"), TuringException);
}

TEST_F(TuringClientTest, RejectsGraphNameWithCrlfRequestLineInjection) {
    // A graph name lands in the request line before the headers, so CR/LF there
    // is an even broader injection point than the token.
    EXPECT_THROW(_client.setGraphName("g HTTP/1.1\r\nHost: evil"), TuringException);
}

TEST_F(TuringClientTest, RejectedGraphNameLeavesPreviousValueIntact) {
    _client.setGraphName("good");
    EXPECT_THROW(_client.setGraphName("bad\r\n"), TuringException);
    EXPECT_EQ(_client.getGraphName(), "good");
}
