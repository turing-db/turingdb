#include <gtest/gtest.h>

#include "TuringDB.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class ShowProceduresTest : public TuringTest {
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

TEST_F(ShowProceduresTest, showProcedures) {
    bool executed = false;
    const auto res = _db->query("SHOW PROCEDURES", "default", &_env->getMem(),
                                [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 2);
        ASSERT_EQ(df->getRowCount(), 4);

        const auto& cols = df->cols();
        const auto* colName = cols.at(0)->as<ColumnVector<types::String::Primitive>>();
        const auto* colSignature = cols.at(1)->as<ColumnVector<std::string>>();

        ASSERT_TRUE(colName != nullptr);
        ASSERT_TRUE(colSignature != nullptr);

        // Check procedure names
        ASSERT_EQ(colName->at(0), "db.labels");
        ASSERT_EQ(colName->at(1), "db.propertyTypes");
        ASSERT_EQ(colName->at(2), "db.edgeTypes");
        ASSERT_EQ(colName->at(3), "db.history");

        // Check exact signatures
        ASSERT_EQ(colSignature->at(0), "db.labels() :: (id :: INTEGER, label :: STRING)");
        ASSERT_EQ(colSignature->at(1),
                  "db.propertyTypes() :: (id :: INTEGER, propertyType :: STRING, valueType :: STRING)");
        ASSERT_EQ(colSignature->at(2), "db.edgeTypes() :: (id :: INTEGER, edgeType :: STRING)");
        ASSERT_EQ(colSignature->at(3),
                  "db.history() :: (commit :: STRING, nodeCount :: INTEGER, edgeCount :: INTEGER, "
                  "partCount :: INTEGER)");

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
