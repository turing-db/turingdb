#include "DBTransactionInfo.h"

#include <optional>

#include <spdlog/fmt/fmt.h>

#include "DBHTTPParams.h"
#include "HTTPParsingInfo.h"
#include "QueryStatus.h"

using namespace db;

void DBTransactionInfo::read(const net::HTTP::Info& httpInfo, DBTransactionInfo& info, QueryStatus& status) {
    const net::HTTP::Params& params = httpInfo.getParams();
    const std::optional<std::string_view>& graphName = params[(size_t)DBHTTPParams::graph];
    const std::optional<std::string_view>& commit = params[(size_t)DBHTTPParams::commit];
    const std::optional<std::string_view>& change = params[(size_t)DBHTTPParams::change];

    info.graphName = graphName.value_or("");
    if (info.graphName.empty()) {
        info.graphName = "default";
    }

    if (commit) {
        const auto commitResult = CommitHash::fromString(commit.value());
        if (!commitResult) {
            status = QueryStatus(QueryStatus::Status::COMMIT_NOT_FOUND,
                                 fmt::format("The commit parameter '{}' is not a hexadecimal commit hash or 'head'", commit.value()));
            return;
        }

        info.commit = commitResult.value();
    }

    if (change) {
        const auto changeResult = ChangeID::fromString(change.value());
        if (!changeResult) {
            status = QueryStatus(QueryStatus::Status::CHANGE_NOT_FOUND,
                                 fmt::format("The change parameter '{}' is not a hexadecimal change ID or 'head'", change.value()));
            return;
        }

        info.change = changeResult.value();
    }
}
