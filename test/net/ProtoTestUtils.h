#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include "QueryCallbacks.h"
#include "iterators/ChunkConfig.h"

namespace db {
class LocalMemory;
class ParameterMap;
class QueryStatus;
}

namespace net::proto {
class TuringAsyncClient;
class TuringClient;
class TuringValueStore;
}

namespace turing::test {

using ResultRows = std::vector<std::vector<std::string>>;

inline constexpr size_t TWO_CHUNK_ROW_COUNT = db::ChunkConfig::CHUNK_SIZE + 1;

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

// Runs the query on a connected async client until its response ends, and asserts the
// query succeeded. Wrap the call in ASSERT_NO_FATAL_FAILURE.
void runAsyncProtoQuery(net::proto::TuringAsyncClient* client,
                        const std::string& query,
                        const db::QueryCallbacks::OnOutputData& callback);

// Runs the query on a connected client through a sink over the caller's value store,
// names the columns the first chunk declares and renders every row as text.
void runProtoQueryOnStore(net::proto::TuringClient* client,
                          db::LocalMemory* localMemory,
                          net::proto::TuringValueStore* values,
                          const std::string& query,
                          std::vector<std::string>& columnNames,
                          ResultRows& rows,
                          db::QueryStatus& status);

}
