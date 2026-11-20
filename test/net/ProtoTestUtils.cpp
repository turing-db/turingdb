#include "ProtoTestUtils.h"

#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <span>

#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryResultFormatter.h"
#include "QueryStatus.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoRequestBody.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"
#include "TuringValueStore.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

namespace turing::test {

void makeProtoRequestBody(const std::string& query, std::string& body) {
    net::proto::TuringProtoRequestBody requestBody;
    requestBody.build(query, nullptr);

    const std::span<char> prefix = requestBody.getPrefix();
    const std::span<char> suffix = requestBody.getSuffix();

    body.assign(prefix.data(), prefix.size());
    body += query;
    body.append(suffix.data(), suffix.size());
}

void makeTooLargeParameters(db::ParameterMap& parameters) {
    constexpr size_t PAIR_COUNT = net::proto::DEFAULT_BUFFER_CAPACITY / (2 * sizeof(int64_t)) + 1;
    constexpr int64_t FIRST_PAIR_VALUE = 1'000'000;

    db::ParameterValue pairs;
    db::ParameterValue::List& pairList = pairs.setList();
    pairList.resize(PAIR_COUNT);

    for (size_t index = 0; index < PAIR_COUNT; index++) {
        const int64_t value = FIRST_PAIR_VALUE + static_cast<int64_t>(index);
        db::ParameterValue::List& pair = pairList[index].setList();
        pair.resize(2);
        pair[0].setInt64(value);
        pair[1].setInt64(value);
    }

    parameters.set("p", pairs);
}

void makeTooLargeParametersMessage(std::string& message) {
    message = fmt::format("The query parameters exceed {} bytes", net::proto::DEFAULT_BUFFER_CAPACITY);
}

void runProtoQuery(net::proto::TuringClient* client,
                   const std::string& query,
                   const db::ParameterMap* parameters,
                   ResultRows& rows,
                   db::QueryStatus& status) {
    std::vector<std::string> values;
    status = client->sendQuery(query, [&rows, &values](const db::Dataframe* dataframe) {
        QueryResultFormatter::appendRows(rows, values, dataframe);
    }, parameters);
}

void runAsyncProtoQuery(net::proto::TuringAsyncClient* client,
                        const std::string& query,
                        const db::QueryCallbacks::OnOutputData& callback) {
    client->sendQuery(query, callback);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!client->isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
        client->send();
        client->recv();
    }

    ASSERT_TRUE(client->isRecvComplete());

    const db::QueryStatus& status = client->getQueryStatus();
    ASSERT_TRUE(status.isOk()) << status.getError();
}

void runProtoQueryOnStore(net::proto::TuringClient* client,
                          db::LocalMemory* localMemory,
                          net::proto::TuringValueStore* values,
                          const std::string& query,
                          std::vector<std::string>& columnNames,
                          ResultRows& rows,
                          db::QueryStatus& status) {
    db::DataframeManager dataframeManager;
    db::Dataframe dataframe;
    net::proto::TuringSink sink(localMemory, values);
    net::proto::TuringSinkColumnContainer container(&dataframe, &dataframeManager);

    bool headerSeen = false;
    std::vector<std::string> rowValues;
    const auto onChunkDecoded = [&columnNames, &rows, &rowValues, &dataframe, &headerSeen]() {
        if (!headerSeen) {
            QueryResultFormatter::appendHeader(columnNames, &dataframe);
            headerSeen = true;
        }

        QueryResultFormatter::appendRows(rows, rowValues, &dataframe);
        dataframe.clear();
    };

    status = client->sendQuery(query, &sink, &container, onChunkDecoded);
}

}
