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
};

struct QueryTestResult {
    std::string name;
    std::string planOutput;
    std::string resultOutput;
    bool planMatched {false};
    bool resultMatched {false};
};

class QueryTestRunner {
public:
    static std::vector<QueryTestSpec> loadTestsFromDir(const fs::Path& dir);

    QueryTestResult runTest(const QueryTestSpec& spec, const fs::Path& outDir);

    static std::string normalizeOutput(std::string_view output);

private:
    static std::string readFile(const fs::Path& path);
};

}
