#pragma once

#include "Path.h"
#include "QueryTestTypes.h"

namespace turing::test {

class RemoteQueryTestRunner {
public:
    QueryTestResult runTest(const QueryTestSpec& spec, const fs::Path& outDir);
};

}
