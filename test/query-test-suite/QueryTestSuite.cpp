#include "TuringTest.h"

#include "QueryTestRunner.h"

namespace turing::test {

class QueryTestSuite : public TuringTest {
public:
    void initialize() override {
        QueryTestRunner::loadTestsFromDir(_tests, fs::Path {QUERY_TEST_SUITE_DIR});
    }

protected:
    std::vector<QueryTestSpec> _tests;
};

TEST_F(QueryTestSuite, RunAll) {
    QueryTestRunner runner;
    size_t executed = 0;
    std::string normalized;

    for (const auto& test : _tests) {
        if (!test.enabled) {
            continue;
        }

        ++executed;
        const fs::Path outDir = fs::Path {_outDir} / test.name;
        const QueryTestResult result = runner.runTest(test, outDir);

        if (!result.planMatched) {
            QueryTestRunner::normalizeOutput(normalized, test.expectPlan);
            ADD_FAILURE() << "Plan output mismatch for test: " << test.name;
            ADD_FAILURE() << "Expected plan:\n"
                          << normalized;
            ADD_FAILURE() << "Actual plan:\n"
                          << result.planOutput;
        }

        if (!result.resultMatched) {
            QueryTestRunner::normalizeOutput(normalized, test.expectResult);
            ADD_FAILURE() << "Result output mismatch for test: " << test.name;
            ADD_FAILURE() << "Expected result:\n"
                          << normalized;
            ADD_FAILURE() << "Actual result:\n"
                          << result.resultOutput;
        }
    }

    EXPECT_GE(executed, 0u);
}

}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 6; });
}
