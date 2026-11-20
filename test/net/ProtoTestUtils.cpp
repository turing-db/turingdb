#include "ProtoTestUtils.h"

#include <stddef.h>
#include <stdint.h>
#include <span>

#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryResultFormatter.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoRequestBody.h"
#include "dataframe/Dataframe.h"

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

}
