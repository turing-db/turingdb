#pragma once

namespace db {

enum class QueryStatus {
    OK,
    PARSE_ERROR,
    ANALYZE_ERROR,
    QUERY_PLAN_ERROR,
    EXEC_ERROR,
    BUFFER_OVERFLOW
};

}
