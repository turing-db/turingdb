#include <gtest/gtest.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class GreeterExtensionTest : public TuringTest {
public:
    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(testTuringDir);
        _db = &_env->getDB();
    }

    auto query(std::string_view q, std::string_view graphName, auto callback) {
        db::QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const db::QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(q, state);
    }

    auto query(std::string_view q, std::string_view graphName) {
        return query(q, graphName, [](const Dataframe*) {});
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(GreeterExtensionTest, callGreeterHello) {
    query("INSTALL greeter", "default");

    bool executed = false;
    const auto res = query("CALL greeter.hello()", "default", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->getLogicalRowCount(), 1);

            const auto& cols = df->cols();
            const auto* msgCol = cols.at(0)->as<ColumnVector<std::string>>();
            ASSERT_TRUE(msgCol != nullptr);
            ASSERT_EQ(msgCol->at(0), "Hello, World!");

            executed = true;
        });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
