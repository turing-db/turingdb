#pragma once

#include <string>
#include <vector>

namespace db {
class ParameterMap;
class QueryStatus;
}

namespace net::proto {
class TuringClient;
}

namespace turing::test {

using ResultRows = std::vector<std::vector<std::string>>;

// Builds the binary body of a query request with no parameters, for tests that post
// the request by hand.
void makeProtoRequestBody(const std::string& query, std::string& body);

// Fills the parameter $p with more pairs of integers than a request body can hold.
void makeTooLargeParameters(db::ParameterMap& parameters);

// The message a request body refuses parameters larger than its buffer with.
void makeTooLargeParametersMessage(std::string& message);

// Runs the query on a connected client and renders every row as text.
void runProtoQuery(net::proto::TuringClient* client,
                   const std::string& query,
                   const db::ParameterMap* parameters,
                   ResultRows& rows,
                   db::QueryStatus& status);

}
