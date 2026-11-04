#include "ProtoTestUtils.h"

#include <span>

#include "QueryResultFormatter.h"
#include "QueryStatus.h"
#include "TuringClient.h"
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
