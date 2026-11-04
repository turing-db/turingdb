#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "RemoteTestUtils.h"
#include "TuringException.h"
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

// A protocol error ends the response with a PROTOCOL_ERROR packet under a Connection: close
// header, and the server closes the connection, so a request sent after it is not answered
class ProtocolErrorClosesConnectionTest : public ProtoServerTest {
protected:
    void expectClosedAfter(const HttpRequest& failing, std::string_view protocolError) {
        HttpResponse response;
        postHttpBody(_port, failing.uri, failing.body, response);

        std::vector<net::proto::MessageTypes> types;
        packetTypes(response.body, types);
        ASSERT_EQ(types.size(), 1u);
        EXPECT_EQ(types[0], net::proto::MessageTypes::PROTOCOL_ERROR);
        EXPECT_NE(response.body.find(protocolError), std::string::npos) << response.body;
        EXPECT_NE(response.headers.find("Connection: close"), std::string::npos) << response.headers;

        std::string query;
        makeEnvelope("MATCH (n) RETURN count(n)", query);

        std::vector<HttpResponse> responses;
        EXPECT_THROW(postHttpBodies(_port, {failing, HttpRequest {"/query?graph=simpledb", query}}, responses), TuringException);
    }
};

TEST_F(ProtocolErrorClosesConnectionTest, closesAfterAnUnsupportedEndpoint) {
    std::string query;
    makeEnvelope("MATCH (n) RETURN count(n)", query);

    expectClosedAfter(HttpRequest {"/nowhere?graph=simpledb", query}, "Unsupported endpoint for the binary protocol");
}

TEST_F(ProtocolErrorClosesConnectionTest, closesAfterAMalformedBody) {
    expectClosedAfter(HttpRequest {"/query?graph=simpledb", "MATCH (n) RETURN n"}, "The request body ends before the end of the query");
}

TEST_F(ProtocolErrorClosesConnectionTest, keepsTheConnectionAfterAnAnsweredQuery) {
    std::string query;
    makeEnvelope("MATCH (n) RETURN count(n)", query);

    std::vector<HttpResponse> responses;
    postHttpBodies(_port, {HttpRequest {"/query?graph=simpledb", query}, HttpRequest {"/query?graph=simpledb", query}}, responses);

    ASSERT_EQ(responses.size(), 2u);
    for (const HttpResponse& response : responses) {
        std::vector<net::proto::MessageTypes> types;
        packetTypes(response.body, types);
        ASSERT_FALSE(types.empty());
        EXPECT_EQ(types.back(), net::proto::MessageTypes::END);
        EXPECT_NE(response.headers.find("Connection: Keep-Alive"), std::string::npos) << response.headers;
    }
}
