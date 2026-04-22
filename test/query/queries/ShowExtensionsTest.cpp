#include <gtest/gtest.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class ShowExtensionsTest : public TuringTest {
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

TEST_F(ShowExtensionsTest, showExtensions) {
    query("INSTALL greeter", "default");

    bool executed = false;
    const auto res = query("SHOW EXTENSIONS", "default", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_GE(df->getLogicalRowCount(), 1);

        const auto& cols = df->cols();
        const auto* colName = cols.at(0)->as<ColumnVector<types::String::Primitive>>();

        ASSERT_TRUE(colName != nullptr);

        // The builtin "greeter" extension should be present
        ASSERT_EQ(colName->at(0), "greeter");

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
