#include <gtest/gtest.h>

#include "TuringDB.h"
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

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
};

TEST_F(ShowExtensionsTest, showExtensions) {
    _db->query("INSTALL greeter", "default", &_env->getMem());

    bool executed = false;
    const auto res = _db->query("SHOW EXTENSIONS", "default", &_env->getMem(),
                                [&](const Dataframe* df) -> void {
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
