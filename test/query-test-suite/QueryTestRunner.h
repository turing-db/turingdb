#pragma once

#include <string_view>

#include "Path.h"
#include "QueryTestTypes.h"

namespace turing::test {

class QueryTestRunner {
public:
    static void loadTestsFromDir(std::vector<QueryTestSpec>& specs,
                                 const fs::Path& dir);

    QueryTestResult runTest(const QueryTestSpec& spec, const fs::Path& outDir);
    static void normalizeOutput(std::string& normalized, std::string_view output);

private:
    static void readFile(std::string& content, const fs::Path& path);
};

} // namespace turing::test
