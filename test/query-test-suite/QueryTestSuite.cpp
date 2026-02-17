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
        if (!test._enabled) {
            continue;
        }

        ++executed;
        const fs::Path outDir = fs::Path {_outDir} / test._name;
        const QueryTestResult result = runner.runTest(test, outDir);

        if (!result._planMatched) {
            QueryTestRunner::normalizeOutput(normalized, test._expectPlan);
            ADD_FAILURE() << "Plan output mismatch for test: " << test._name;
            ADD_FAILURE() << "Expected plan:\n"
                          << normalized;
            ADD_FAILURE() << "Actual plan:\n"
                          << result._planOutput;
        }

        if (!result._resultMatched) {
            QueryTestRunner::normalizeOutput(normalized, test._expectResult);
            ADD_FAILURE() << "Result output mismatch for test: " << test._name;
            ADD_FAILURE() << "Expected result:\n"
                          << normalized;
            ADD_FAILURE() << "Actual result:\n"
                          << result._resultOutput;
        }

        if (!result._resultJsonValid) {
            ADD_FAILURE() << "Result JSON invalid for test: "
                          << test._name << "\n"
                          << result._resultJsonError;
            ADD_FAILURE() << "JSON output:\n"
                          << result._resultJsonOutput;
        }

        if (!result._resultJsonMatched) {
            ADD_FAILURE() << "Result JSON mismatch for test: "
                          << test._name;
            ADD_FAILURE() << "Expected JSON:\n"
                          << test._expectResultJson;
            ADD_FAILURE() << "Actual JSON:\n"
                          << result._resultJsonOutput;
        }
    }

    EXPECT_GE(executed, 0u);
}

}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 6; });
}
