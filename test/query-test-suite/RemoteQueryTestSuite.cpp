#include <algorithm>

#include "TuringTest.h"

#include "QueryTestRunner.h"
#include "RemoteQueryTestRunner.h"

namespace turing::test {

class RemoteQueryTestSuite : public TuringTest {
public:
    void initialize() override {
        QueryTestRunner::loadTestsFromDir(_tests, fs::Path {QUERY_TEST_SUITE_DIR});
        // Keep the remote suite aligned with the CLI by filtering out fixtures
        // that are disabled globally or explicitly marked local-only.
        std::erase_if(_tests, [](const QueryTestSpec& spec) {
            return !spec._enabled || !spec._remoteEnabled;
        });
    }

protected:
    std::vector<QueryTestSpec> _tests;
};

TEST_F(RemoteQueryTestSuite, RunAllEnabled) {
    RemoteQueryTestRunner runner;
    size_t executed = 0;

    for (const auto& test : _tests) {
        if (!test._enabled || !test._remoteEnabled) {
            continue;
        }

        ++executed;
        const fs::Path outDir = fs::Path {_outDir} / test._name;
        const QueryTestResult result = runner.runTest(test, outDir);

        if (!result._resultMatched) {
            ADD_FAILURE() << "Result output mismatch for remote test: " << test._name;
            ADD_FAILURE() << "Expected result:\n"
                          << test._expectResult;
            ADD_FAILURE() << "Actual result:\n"
                          << result._resultOutput;
        }
    }

    EXPECT_EQ(executed, _tests.size());
}

}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 1; });
}
