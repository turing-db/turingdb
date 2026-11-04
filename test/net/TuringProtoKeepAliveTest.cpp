#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "RemoteTestUtils.h"
#include "TuringProtoHeaders.h"

using namespace db;
using namespace turing::test;

namespace {

void makeEnvelope(std::string_view query, std::string& body) {
    const net::proto::WireSize queryLength = static_cast<net::proto::WireSize>(query.size());
    const net::proto::WireSize parameterLength = 0;

    body.append(reinterpret_cast<const char*>(&queryLength), sizeof(queryLength));
    body.append(query);
    body.append(reinterpret_cast<const char*>(&parameterLength), sizeof(parameterLength));
}

void packetTypes(const std::string& body, std::vector<net::proto::MessageTypes>& types) {
    size_t position = 0;
    while (position < body.size()) {
        const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(body.data() + position, body.size() - position);
        types.push_back(header._type);
        position += net::proto::ProtoHeader::wireSize() + header._dataLen;
    }
}

}

// A protocol error is answered in full under the Keep-Alive the response opens with, so the
// request after it on the same connection is answered too
class TuringProtoKeepAliveTest : public ProtoServerTest {
protected:
    void expectAnsweredAfter(const HttpRequest& failing, std::string_view protocolError) {
        std::string query;
        makeEnvelope("MATCH (n) RETURN count(n)", query);

        std::vector<HttpResponse> responses;
        postHttpBodies(_port, {failing, HttpRequest {"/query?graph=simpledb", query}}, responses);
        ASSERT_EQ(responses.size(), 2u);

        std::vector<net::proto::MessageTypes> failingTypes;
        packetTypes(responses[0].body, failingTypes);
        ASSERT_EQ(failingTypes.size(), 1u);
        EXPECT_EQ(failingTypes[0], net::proto::MessageTypes::PROTOCOL_ERROR);
        EXPECT_NE(responses[0].body.find(protocolError), std::string::npos) << responses[0].body;

        std::vector<net::proto::MessageTypes> answeredTypes;
        packetTypes(responses[1].body, answeredTypes);
        ASSERT_FALSE(answeredTypes.empty());
        EXPECT_EQ(answeredTypes.front(), net::proto::MessageTypes::CHUNK_HEADER);
        EXPECT_EQ(answeredTypes.back(), net::proto::MessageTypes::END);
    }
};

TEST_F(TuringProtoKeepAliveTest, answersTheRequestAfterAnUnsupportedEndpoint) {
    std::string query;
    makeEnvelope("MATCH (n) RETURN count(n)", query);

    expectAnsweredAfter(HttpRequest {"/nowhere?graph=simpledb", query}, "Unsupported endpoint for the binary protocol");
}

TEST_F(TuringProtoKeepAliveTest, answersTheRequestAfterAMalformedBody) {
    expectAnsweredAfter(HttpRequest {"/query?graph=simpledb", "MATCH (n) RETURN n"}, "The request body ends before the end of the query");
}
