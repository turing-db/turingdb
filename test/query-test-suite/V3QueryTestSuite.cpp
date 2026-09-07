#include "TuringTest.h"

#include "QueryTestRunner.h"
#include "V3QueryTestRunner.h"

namespace turing::test {

class V3QueryTestSuite : public TuringTest {
public:
    void initialize() override {
        QueryTestRunner::loadTestsFromDir(_tests, fs::Path {QUERY_TEST_SUITE_DIR});
    }

protected:
    std::vector<QueryTestSpec> _tests;
};

TEST_F(V3QueryTestSuite, RunAll) {
    V3QueryTestRunner runner;
    size_t executed = 0;
    std::string normalized;

    for (const auto& test : _tests) {
        if (!test._enabled) {
            continue;
        }

        ++executed;
        const fs::Path outDir = fs::Path {_outDir} / test._name;
        const V3QueryTestResult result = runner.runTest(test, outDir);

        if (!result._resultMatched) {
            QueryTestRunner::normalizeOutput(normalized, test._expectResult);
            ADD_FAILURE() << "V3 result output mismatch for test: " << test._name;
            ADD_FAILURE() << "Expected result:\n"
                          << normalized;
            ADD_FAILURE() << "Actual result:\n"
                          << result._resultOutput;
        }
    }

    EXPECT_GE(executed, 0u);
}

}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}
