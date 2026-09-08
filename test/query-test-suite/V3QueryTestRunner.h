#pragma once

#include "Path.h"
#include "QueryTestTypes.h"

namespace turing::test {

class V3QueryTestRunner {
public:
    V3QueryTestResult runTest(const QueryTestSpec& spec, const fs::Path& outDir);
};

}
