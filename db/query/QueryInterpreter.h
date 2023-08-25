#pragma once

#include <string>
#include <optional>
#include <vector>

class PullResponse;

namespace db::query {

class PullPlan;

class QueryInterpreter {
public:
    struct PrepareResult {
        std::optional<size_t> _qid;
        bool _success {false};
    };

    struct QueryExecution {
        QueryExecution(PullPlan* plan);
        ~QueryExecution();

        PullPlan* _plan {nullptr};
    };

    QueryInterpreter();
    ~QueryInterpreter();

    PrepareResult prepare(const std::string& query);

    bool pull(PullResponse* result, size_t qid);

private:
    std::vector<QueryExecution> _queries;

    size_t registerQuery(PullPlan* plan);
};

}
