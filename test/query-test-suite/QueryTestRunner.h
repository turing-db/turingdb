#pragma once

#include <string_view>

#include "Path.h"
#include "QueryTestTypes.h"

namespace turing::test {

class QueryTestRunner {
public:
    static void loadTestsFromDir(std::vector<QueryTestSpec>& specs,
                                 const fs::Path& dir);

    static void normalizeOutput(std::string& normalized, std::string_view output);

    static void splitStatements(std::vector<std::string>& statements, std::string_view script);

    static void readFile(std::string& content, const fs::Path& path);
};

}
