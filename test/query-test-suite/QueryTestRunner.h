#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "Path.h"

namespace turing::test {

struct QueryTestSpec {
    std::string name;
    std::string graphName {"simpledb"};
    std::string query;
    std::string expectPlan;
    std::string expectResult;
    std::vector<std::string> tags;
    bool enabled {true};
    bool writeRequired {false};
    std::string disabledReason;
};

struct QueryTestResult {
    std::string name;
    std::string planOutput;
    std::string resultOutput;
    bool planMatched {false};
    bool resultMatched {false};
    uint64_t timeUs {0};
};

class QueryTestRunner {
public:
    static void loadTestsFromDir(std::vector<QueryTestSpec>& specs, const fs::Path& dir);

    QueryTestResult runTest(const QueryTestSpec& spec, const fs::Path& outDir);

    static void normalizeOutput(std::string& normalized, std::string_view output);

private:
    static void readFile(std::string& content, const fs::Path& path);
};

}
