#include "DBQueryRequest.h"

#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

#include "HTTPParsingInfo.h"
#include "ParameterJson.h"
#include "QueryStatus.h"

#include "TuringException.h"

using namespace db;

DBQueryRequest::DBQueryRequest()
{
}

DBQueryRequest::~DBQueryRequest() {
}

void DBQueryRequest::read(const net::HTTP::Info& httpInfo, DBQueryRequest& request, QueryStatus& status) {
    const nlohmann::json body = nlohmann::json::parse(httpInfo.getPayload(), nullptr, false);
    if (body.is_discarded() || !body.is_object()) {
        status = QueryStatus(QueryStatus::Status::PARSE_ERROR, "The request body must be a JSON object");
        return;
    }

    for (const auto& [key, value] : body.items()) {
        if (key == "query") {
            if (!value.is_string()) {
                status = QueryStatus(QueryStatus::Status::PARSE_ERROR, "The query field must be a string");
                return;
            }

            request._query = value.get<std::string>();
        } else if (key == "parameters") {
            try {
                readJsonParameters(value, request._parameters);
            } catch (const TuringException& e) {
                status = QueryStatus(QueryStatus::Status::PARSE_ERROR, e.what());
                return;
            }
        } else {
            status = QueryStatus(QueryStatus::Status::PARSE_ERROR,
                                 fmt::format("Unknown field '{}' in the request body", key));
            return;
        }
    }

    if (!body.contains("query")) {
        status = QueryStatus(QueryStatus::Status::PARSE_ERROR, "The request body has no query field");
    }
}
