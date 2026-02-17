#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "Path.h"

namespace turing::test {

struct QueryTestSpec {
    std::string _name;
    std::string _graphName {"simpledb"};
    std::string _query;
    std::string _expectPlan;
    std::string _expectResult;
    std::string _expectResultJson;
    std::vector<std::string> _tags;
    bool _enabled {true};
    bool _writeRequired {false};
    std::string _disabledReason;
};

struct QueryTestResult {
    std::string _name;
    std::string _planOutput;
    std::string _resultOutput;
    std::string _resultJsonOutput;
    std::string _resultJsonError;
    bool _planMatched {false};
    bool _resultMatched {false};
    bool _resultJsonMatched {false};
    bool _resultJsonValid {false};
    uint64_t _timeUs {0};
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
