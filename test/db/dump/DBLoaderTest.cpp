#include "DBLoader.h"
#include "BioLog.h"
#include "DB.h"
#include "DBDumper.h"
#include "FileUtils.h"
#include "JsonExamples.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "Writeback.h"
#include "gtest/gtest.h"

namespace db {

class DBLoaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        Log::BioLog::init();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";

        // Remove the directory from the previous run
        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }

        _db = cyberSecurityDB();

        DBDumper dumper(_db, _outDir);
        dumper.dump();
    }

    void TearDown() override {
        if (_db) {
            delete _db;
        }

        Log::BioLog::destroy();
    }

    DB* _db{nullptr};
    std::string _outDir;
};

TEST_F(DBLoaderTest, LoadDB) {
    DB* db = DB::create();
    DBLoader loader(db, _outDir);
    Writeback wb1{_db};
    Writeback wb2{db};

    ASSERT_TRUE(loader.load());
    ASSERT_TRUE(Comparator<DB>::same(_db, db));

    Network* net1 = _db->getNetwork(_db->getString("Neo4jNetwork"));
    Network* net2 = db->getNetwork(db->getString("Neo4jNetwork"));
    ASSERT_TRUE(net1);
    ASSERT_TRUE(net2);

    NodeType* nt1 = wb1.createNodeType(_db->getString("NodeType"));
    ASSERT_TRUE(nt1);
    ASSERT_FALSE(Comparator<DB>::same(_db, db));
    NodeType* nt2 = wb2.createNodeType(db->getString("NodeType"));
    ASSERT_TRUE(nt2);
    ASSERT_TRUE(Comparator<DB>::same(_db, db));

    EdgeType* et1 = wb1.createEdgeType(_db->getString("EdgeType"), nt1, nt1);
    ASSERT_TRUE(et1);
    ASSERT_FALSE(Comparator<DB>::same(_db, db));
    EdgeType* et2 = wb2.createEdgeType(db->getString("EdgeType"), nt2, nt2);
    ASSERT_TRUE(et2);
    ASSERT_TRUE(Comparator<DB>::same(_db, db));

    Node* n11 = wb1.createNode(net1, nt1, _db->getString("Node1"));
    Node* n12 = wb1.createNode(net1, nt1, _db->getString("Node2"));
    ASSERT_TRUE(n11);
    ASSERT_TRUE(n12);
    ASSERT_FALSE(Comparator<DB>::same(_db, db));
    Node* n21 = wb2.createNode(net2, nt2, db->getString("Node1"));
    Node* n22 = wb2.createNode(net2, nt2, db->getString("Node2"));
    ASSERT_TRUE(n21);
    ASSERT_TRUE(n22);
    ASSERT_TRUE(Comparator<DB>::same(_db, db));

    Edge* e1 = wb1.createEdge(et1, n11, n12);
    ASSERT_TRUE(e1);
    ASSERT_FALSE(Comparator<DB>::same(_db, db));
    Edge* e2 = wb2.createEdge(et2, n21, n22);
    ASSERT_TRUE(e2);
    ASSERT_TRUE(Comparator<DB>::same(_db, db));

    // The order in which objects are pushed into the database matters1
    // For example:
    // NodeType first then EdgeType
    wb1.createNodeType(_db->getString("NodeType2"));
    wb1.createEdgeType(_db->getString("EdgeType2"), nt1, nt1);
    // EdgeType then NodeType
    wb2.createEdgeType(db->getString("EdgeType2"), nt2, nt2);
    wb2.createNodeType(db->getString("NodeType2"));
    ASSERT_FALSE(Comparator<DB>::same(_db, db));

    delete db;
}

}
