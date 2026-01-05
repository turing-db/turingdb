#include <filesystem>
#include <gtest/gtest.h>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "dataframe/Dataframe.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class CreateGraphTest : public TuringTest {
public:
    void initialize() override {
        const auto testTuringDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(testTuringDir);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
};

TEST_F(CreateGraphTest, createGraph) {
    bool executed = false;
    const auto res = _db->query("CREATE GRAPH testDB", "default", &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_EQ(df->getRowCount(), 1);
        const auto& cols = df->cols();
        const auto* colName = cols.at(0)->as<ColumnConst<types::String::Primitive>>();
        ASSERT_EQ(colName->getRaw(), "testDB");
        executed = true;
    });

    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);

    const auto graphDir = FileUtils::Path {_outDir} / "turing" / "graphs" / "testDB";
    ASSERT_TRUE(FileUtils::exists(graphDir));
    ASSERT_TRUE(FileUtils::isDirectory(graphDir));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
