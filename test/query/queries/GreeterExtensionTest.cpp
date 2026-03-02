#include <gtest/gtest.h>

#include "TuringDB.h"
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

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
};

TEST_F(GreeterExtensionTest, callGreeterHello) {
    _db->query("INSTALL greeter", "default", &_env->getMem());

    bool executed = false;
    const auto res = _db->query(
        "CALL greeter.hello()", "default", &_env->getMem(),
        [&](const Dataframe* df) -> void {
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
