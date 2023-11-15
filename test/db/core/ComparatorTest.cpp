#include "Comparator.h"
#include "BioLog.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeType.h"
#include "FileUtils.h"
#include "JsonExamples.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "PropertyType.h"
#include "Writeback.h"

#include <gtest/gtest.h>

using namespace db;

class ComparatorTest : public ::testing::Test, protected DBComparator {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());
    }

    void TearDown() override {
        Log::BioLog::destroy();
    }

    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ComparatorTest, ValueComparison) {
    Value v1 = Value::createInt(5);
    Value v2 = Value::createUnsigned(5);
    Value v3 = Value::createInt(6);
    Value v4 = Value::createInt(5);
    ASSERT_TRUE(DBComparator::same<Value>(&v1, &v1));
    ASSERT_FALSE(DBComparator::same<Value>(&v1, &v2));
    ASSERT_FALSE(DBComparator::same<Value>(&v1, &v3));
    ASSERT_TRUE(DBComparator::same<Value>(&v1, &v4));
}

TEST_F(ComparatorTest, ValueTypeComparison) {
    ValueType vt1 {ValueType::VK_INT};
    ValueType vt2 {ValueType::VK_UNSIGNED};
    ValueType vt3 {ValueType::VK_INT};
    ASSERT_TRUE(DBComparator::same<ValueType>(&vt1, &vt1));
    ASSERT_FALSE(DBComparator::same<ValueType>(&vt1, &vt2));
    ASSERT_TRUE(DBComparator::same<ValueType>(&vt1, &vt3));
}

TEST_F(ComparatorTest, PropertyTypeComparison) {
    // First DB
    DB* db = DB::create();
    Writeback wb(db);
    NodeType* nt1 = wb.createNodeType(db->getString("NodeType1"));
    NodeType* nt2 = wb.createNodeType(db->getString("NodeType2"));

    PropertyType* pType1 = wb.addPropertyType(nt1, db->getString("PropType1"),
                                              ValueType::stringType());
    PropertyType* pType2 = wb.addPropertyType(nt1, db->getString("PropType2"),
                                              ValueType::stringType());
    PropertyType* pType3 = wb.addPropertyType(nt2, db->getString("PropType1"),
                                              ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<DBType>((DBType*)pType1, (DBType*)pType1));
    ASSERT_FALSE(DBComparator::same<DBType>((DBType*)pType1, (DBType*)pType2));

    ASSERT_TRUE(DBComparator::same<DBObject>((DBObject*)pType1, (DBObject*)pType1));
    ASSERT_FALSE(DBComparator::same<DBObject>((DBObject*)pType1, (DBObject*)pType2));

    ASSERT_TRUE(DBComparator::same<PropertyType>(pType1, pType1));
    ASSERT_FALSE(DBComparator::same<PropertyType>(pType1, pType2));
    ASSERT_FALSE(DBComparator::same<PropertyType>(pType1, pType3));

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2(db2);
    NodeType* nt1_2 = wb2.createNodeType(db2->getString("NodeType1"));
    NodeType* nt2_2 = wb2.createNodeType(db2->getString("NodeType2"));

    PropertyType* pType1_2 = wb2.addPropertyType(nt1_2, db2->getString("PropType1"),
                                                 ValueType::stringType());
    PropertyType* pType2_2 = wb2.addPropertyType(nt1_2, db2->getString("PropType2"),
                                                 ValueType::stringType());
    PropertyType* pType3_2 = wb2.addPropertyType(nt2_2, db2->getString("PropType1"),
                                                 ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<PropertyType>(pType1, pType1_2));
    ASSERT_TRUE(DBComparator::same<PropertyType>(pType2, pType2_2));
    ASSERT_TRUE(DBComparator::same<PropertyType>(pType3, pType3_2));

    delete db;
    delete db2;
}

TEST_F(ComparatorTest, NodeTypeComparison) {
    // First DB
    DB* db = DB::create();
    Writeback wb(db);
    NodeType* nt1 = wb.createNodeType(db->getString("NodeType1"));
    NodeType* nt2 = wb.createNodeType(db->getString("NodeType2"));
    NodeType* nt3 = wb.createNodeType(db->getString("NodeType3"));
    NodeType* nt4 = wb.createNodeType(db->getString("NodeType4"));

    wb.addPropertyType(nt1, db->getString("PropType1"), ValueType::stringType());
    wb.addPropertyType(nt1, db->getString("PropType2"), ValueType::stringType());
    wb.addPropertyType(nt2, db->getString("PropType1"), ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<DBEntityType>((DBEntityType*)nt1, (DBEntityType*)nt1));
    ASSERT_FALSE(DBComparator::same<DBEntityType>((DBEntityType*)nt1, (DBEntityType*)nt2));

    ASSERT_TRUE(DBComparator::same<DBType>((DBType*)nt1, (DBType*)nt1));
    ASSERT_FALSE(DBComparator::same<DBType>((DBType*)nt1, (DBType*)nt2));

    ASSERT_TRUE(DBComparator::same<DBObject>((DBObject*)nt1, (DBObject*)nt1));
    ASSERT_FALSE(DBComparator::same<DBObject>((DBObject*)nt1, (DBObject*)nt2));

    ASSERT_TRUE(DBComparator::same<NodeType>(nt1, nt1));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt2, nt2));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt3, nt3));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt4, nt4));
    ASSERT_FALSE(DBComparator::same<NodeType>(nt1, nt2));
    ASSERT_FALSE(DBComparator::same<NodeType>(nt1, nt3));
    ASSERT_FALSE(DBComparator::same<NodeType>(nt1, nt4));

    // Second DB
    DB* db2 = DB::create();

    Writeback wb2(db2);
    NodeType* nt1_2 = wb2.createNodeType(db2->getString("NodeType1"));
    NodeType* nt2_2 = wb2.createNodeType(db2->getString("NodeType2"));
    NodeType* nt3_2 = wb2.createNodeType(db2->getString("NodeType3"));
    NodeType* nt4_2 = wb2.createNodeType(db2->getString("NodeType4"));

    wb2.addPropertyType(nt1_2, db2->getString("PropType1"), ValueType::stringType());
    wb2.addPropertyType(nt1_2, db2->getString("PropType2"), ValueType::stringType());
    wb2.addPropertyType(nt2_2, db2->getString("PropType1"), ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<NodeType>(nt1, nt1_2));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt2, nt2_2));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt3, nt3_2));
    ASSERT_TRUE(DBComparator::same<NodeType>(nt4, nt4_2));

    delete db;
    delete db2;
}

TEST_F(ComparatorTest, EdgeTypeComparison) {
    // First DB
    DB* db = DB::create();
    Writeback wb(db);
    NodeType* nt1 = wb.createNodeType(db->getString("NodeType1"));
    NodeType* nt2 = wb.createNodeType(db->getString("NodeType2"));

    EdgeType* et1 = wb.createEdgeType(db->getString("EdgeType1"), nt1, nt1);
    EdgeType* et2 = wb.createEdgeType(db->getString("EdgeType2"), nt1, nt1);
    EdgeType* et3 = wb.createEdgeType(db->getString("EdgeType3"), nt1, nt2);
    EdgeType* et4 = wb.createEdgeType(db->getString("EdgeType4"), nt1, nt2);

    wb.addPropertyType(et1, db->getString("PropType1"),
                       ValueType::stringType());
    wb.addPropertyType(et1, db->getString("PropType2"),
                       ValueType::stringType());
    wb.addPropertyType(et2, db->getString("PropType1"),
                       ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<DBEntityType>((DBEntityType*)et1, (DBEntityType*)et1));
    ASSERT_FALSE(DBComparator::same<DBEntityType>((DBEntityType*)et1, (DBEntityType*)et2));

    ASSERT_TRUE(DBComparator::same<DBType>((DBType*)et1, (DBType*)et1));
    ASSERT_FALSE(DBComparator::same<DBType>((DBType*)et1, (DBType*)et2));

    ASSERT_TRUE(DBComparator::same<DBObject>((DBObject*)et1, (DBObject*)et1));
    ASSERT_FALSE(DBComparator::same<DBObject>((DBObject*)et1, (DBObject*)et2));

    ASSERT_TRUE(DBComparator::same<EdgeType>(et1, et1));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et2, et2));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et3, et3));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et4, et4));
    ASSERT_FALSE(DBComparator::same<EdgeType>(et1, et2));
    ASSERT_FALSE(DBComparator::same<EdgeType>(et1, et3));
    ASSERT_FALSE(DBComparator::same<EdgeType>(et1, et4));

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2(db2);
    NodeType* nt1_2 = wb2.createNodeType(db2->getString("NodeType1"));
    NodeType* nt2_2 = wb2.createNodeType(db2->getString("NodeType2"));

    EdgeType* et1_2 = wb2.createEdgeType(db2->getString("EdgeType1"), nt1_2, nt1_2);
    EdgeType* et2_2 = wb2.createEdgeType(db2->getString("EdgeType2"), nt1_2, nt1_2);
    EdgeType* et3_2 = wb2.createEdgeType(db2->getString("EdgeType3"), nt1_2, nt2_2);
    EdgeType* et4_2 = wb2.createEdgeType(db2->getString("EdgeType4"), nt1_2, nt2_2);

    wb2.addPropertyType(et1_2, db2->getString("PropType1"),
                        ValueType::stringType());
    wb2.addPropertyType(et1_2, db2->getString("PropType2"),
                        ValueType::stringType());
    wb2.addPropertyType(et2_2, db2->getString("PropType1"),
                        ValueType::stringType());

    ASSERT_TRUE(DBComparator::same<EdgeType>(et1, et1_2));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et2, et2_2));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et3, et3_2));
    ASSERT_TRUE(DBComparator::same<EdgeType>(et4, et4_2));

    delete db;
    delete db2;
}

TEST_F(ComparatorTest, PropertyComparison) {
    // First DB
    DB* db = DB::create();
    Writeback wb {db};
    NodeType* nt = wb.createNodeType(db->getString("NodeType"));

    PropertyType* pType1 = wb.addPropertyType(nt, db->getString("PropType1"),
                                              ValueType::intType());
    PropertyType* pType2 = wb.addPropertyType(nt, db->getString("PropType2"),
                                              ValueType::intType());
    PropertyType* pType3 = wb.addPropertyType(nt, db->getString("PropType3"),
                                              ValueType::unsignedType());

    Property p1 {pType1, Value::createInt(5)};
    Property p2 {pType1, Value::createInt(6)};
    Property p3 {pType3, Value::createUnsigned(5)};
    Property p4 {pType2, Value::createInt(5)};

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2 {db2};
    NodeType* nt2 = wb2.createNodeType(db2->getString("NodeType"));

    PropertyType* pType1_2 = wb2.addPropertyType(nt2, db2->getString("PropType1"),
                                                 ValueType::intType());
    PropertyType* pType2_2 = wb2.addPropertyType(nt2, db2->getString("PropType2"),
                                                 ValueType::intType());
    PropertyType* pType3_2 = wb2.addPropertyType(nt2, db2->getString("PropType3"),
                                                 ValueType::unsignedType());

    Property p1_2 {pType1_2, Value::createInt(5)};
    Property p2_2 {pType1_2, Value::createInt(6)};
    Property p3_2 {pType3_2, Value::createUnsigned(5)};
    Property p4_2 {pType2_2, Value::createInt(5)};

    delete db;
    delete db2;
}

TEST_F(ComparatorTest, NetworkComparison) {
    // First DB
    DB* db = DB::create();
    Writeback wb {db};
    Network* net1 = wb.createNetwork(db->getString("Network1"));
    Network* net2 = wb.createNetwork(db->getString("Network2"));
    ASSERT_TRUE(DBComparator::same<Network>(net1, net1));
    ASSERT_FALSE(DBComparator::same<Network>(net1, net2));

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2 {db2};
    Network* net1_2 = wb2.createNetwork(db2->getString("Network1"));
    Network* net2_2 = wb2.createNetwork(db2->getString("Network2"));
    ASSERT_TRUE(DBComparator::same<Network>(net1, net1_2));
    ASSERT_TRUE(DBComparator::same<Network>(net2, net2_2));

    delete db;
    delete db2;
}

TEST_F(ComparatorTest, NodeComparison) {
    // First DB
    DB* db1 = DB::create();
    Writeback wb1 {db1};
    Network* net1 = wb1.createNetwork(db1->getString("Network"));

    NodeType* nt1_1 = wb1.createNodeType(db1->getString("NodeType1"));
    PropertyType* pt1_1 = wb1.addPropertyType(nt1_1, db1->getString("PropType1"),
                                              ValueType::intType());
    PropertyType* pt2_1 = wb1.addPropertyType(nt1_1, db1->getString("PropType2"),
                                              ValueType::unsignedType());

    NodeType* nt2_1 = wb1.createNodeType(db1->getString("NodeType2"));
    PropertyType* pt3_1 = wb1.addPropertyType(nt2_1, db1->getString("PropType3"),
                                              ValueType::intType());
    PropertyType* pt4_1 = wb1.addPropertyType(nt2_1, db1->getString("PropType4"),
                                              ValueType::unsignedType());

    Node* n1_1 = wb1.createNode(net1, nt1_1, db1->getString("Node1"));
    wb1.setProperty(n1_1, Property {pt1_1, Value::createInt(5)});
    wb1.setProperty(n1_1, Property {pt2_1, Value::createUnsigned(3)});
    Node* n2_1 = wb1.createNode(net1, nt2_1, db1->getString("Node2"));
    wb1.setProperty(n2_1, Property {pt3_1, Value::createInt(2)});
    wb1.setProperty(n2_1, Property {pt4_1, Value::createUnsigned(7)});
    Node* n3_1 = wb1.createNode(net1, nt1_1, db1->getString("Node3"));
    wb1.setProperty(n3_1, Property {pt1_1, Value::createInt(2)});
    wb1.setProperty(n3_1, Property {pt2_1, Value::createUnsigned(1)});

    ASSERT_TRUE(DBComparator::same<Node>(n1_1, n1_1));
    ASSERT_TRUE(DBComparator::same<Node>(n2_1, n2_1));
    ASSERT_TRUE(DBComparator::same<Node>(n3_1, n3_1));
    ASSERT_FALSE(DBComparator::same<Node>(n1_1, n2_1));
    ASSERT_FALSE(DBComparator::same<Node>(n1_1, n3_1));

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2 {db2};
    Network* net2 = wb2.createNetwork(db2->getString("Network"));

    NodeType* nt1_2 = wb2.createNodeType(db2->getString("NodeType1"));
    PropertyType* pt1_2 = wb2.addPropertyType(nt1_2, db2->getString("PropType1"),
                                              ValueType::intType());
    PropertyType* pt2_2 = wb2.addPropertyType(nt1_2, db2->getString("PropType2"),
                                              ValueType::unsignedType());

    NodeType* nt2_2 = wb2.createNodeType(db2->getString("NodeType2"));
    PropertyType* pt3_2 = wb2.addPropertyType(nt2_2, db2->getString("PropType3"),
                                              ValueType::intType());
    PropertyType* pt4_2 = wb2.addPropertyType(nt2_2, db2->getString("PropType4"),
                                              ValueType::unsignedType());

    Node* n1_2 = wb2.createNode(net2, nt1_2, db2->getString("Node1"));
    wb2.setProperty(n1_2, Property {pt1_2, Value::createInt(5)});
    wb2.setProperty(n1_2, Property {pt2_2, Value::createUnsigned(3)});
    Node* n2_2 = wb2.createNode(net2, nt2_2, db2->getString("Node2"));
    wb2.setProperty(n2_2, Property {pt3_2, Value::createInt(2)});
    wb2.setProperty(n2_2, Property {pt4_2, Value::createUnsigned(7)});
    Node* n3_2 = wb2.createNode(net2, nt1_2, db2->getString("Node3"));
    wb2.setProperty(n3_2, Property {pt1_2, Value::createInt(2)});
    wb2.setProperty(n3_2, Property {pt2_2, Value::createUnsigned(1)});

    ASSERT_TRUE(DBComparator::same<Node>(n1_1, n1_2));
    ASSERT_TRUE(DBComparator::same<Node>(n2_1, n2_2));
    ASSERT_TRUE(DBComparator::same<Node>(n3_1, n3_2));

    delete db1;
    delete db2;
}

TEST_F(ComparatorTest, EdgeComparison) {
    // First DB
    DB* db1 = DB::create();
    Writeback wb1 {db1};
    Network* net1 = wb1.createNetwork(db1->getString("Network"));

    NodeType* nt1_1 = wb1.createNodeType(db1->getString("NodeType1"));
    NodeType* nt2_1 = wb1.createNodeType(db1->getString("NodeType2"));
    Node* n1_1 = wb1.createNode(net1, nt1_1);
    Node* n2_1 = wb1.createNode(net1, nt1_1);
    Node* n3_1 = wb1.createNode(net1, nt2_1);

    EdgeType* et1_1 = wb1.createEdgeType(db1->getString("EdgeType1"), nt1_1, nt1_1);
    EdgeType* et2_1 = wb1.createEdgeType(db1->getString("EdgeType2"), nt1_1, nt2_1);

    PropertyType* pt1_1 = wb1.addPropertyType(et1_1, db1->getString("PropType1"),
                                              ValueType::intType());
    PropertyType* pt2_1 = wb1.addPropertyType(et1_1, db1->getString("PropType2"),
                                              ValueType::unsignedType());
    PropertyType* pt3_1 = wb1.addPropertyType(et2_1, db1->getString("PropType3"),
                                              ValueType::intType());
    PropertyType* pt4_1 = wb1.addPropertyType(et2_1, db1->getString("PropType4"),
                                              ValueType::unsignedType());

    Edge* e1_1 = wb1.createEdge(et1_1, n1_1, n2_1);
    Edge* e2_1 = wb1.createEdge(et2_1, n1_1, n3_1);

    wb1.setProperty(e1_1, Property {pt1_1, Value::createInt(5)});
    wb1.setProperty(e1_1, Property {pt2_1, Value::createUnsigned(3)});
    wb1.setProperty(e2_1, Property {pt3_1, Value::createInt(2)});
    wb1.setProperty(e2_1, Property {pt4_1, Value::createUnsigned(7)});

    ASSERT_TRUE(DBComparator::same<Edge>(e1_1, e1_1));
    ASSERT_TRUE(DBComparator::same<Edge>(e2_1, e2_1));
    ASSERT_FALSE(DBComparator::same<Edge>(e1_1, e2_1));

    // Second DB
    DB* db2 = DB::create();
    Writeback wb2 {db2};
    Network* net2 = wb2.createNetwork(db2->getString("Network"));

    NodeType* nt1_2 = wb2.createNodeType(db2->getString("NodeType1"));
    NodeType* nt2_2 = wb2.createNodeType(db2->getString("NodeType2"));
    Node* n1_2 = wb2.createNode(net2, nt1_2);
    Node* n2_2 = wb2.createNode(net2, nt1_2);
    Node* n3_2 = wb2.createNode(net2, nt2_2);

    EdgeType* et1_2 = wb2.createEdgeType(db2->getString("EdgeType1"), nt1_2, nt1_2);
    EdgeType* et2_2 = wb2.createEdgeType(db2->getString("EdgeType2"), nt1_2, nt2_2);

    PropertyType* pt1_2 = wb2.addPropertyType(et1_2, db2->getString("PropType1"),
                                              ValueType::intType());
    PropertyType* pt2_2 = wb2.addPropertyType(et1_2, db2->getString("PropType2"),
                                              ValueType::unsignedType());
    PropertyType* pt3_2 = wb2.addPropertyType(et2_2, db2->getString("PropType3"),
                                              ValueType::intType());
    PropertyType* pt4_2 = wb2.addPropertyType(et2_2, db2->getString("PropType4"),
                                              ValueType::unsignedType());

    Edge* e1_2 = wb2.createEdge(et1_2, n1_2, n2_2);
    Edge* e2_2 = wb2.createEdge(et2_2, n1_2, n3_2);

    wb2.setProperty(e1_2, Property {pt1_2, Value::createInt(5)});
    wb2.setProperty(e1_2, Property {pt2_2, Value::createUnsigned(3)});
    wb2.setProperty(e2_2, Property {pt3_2, Value::createInt(2)});
    wb2.setProperty(e2_2, Property {pt4_2, Value::createUnsigned(7)});

    ASSERT_TRUE(DBComparator::same<Edge>(e1_1, e1_2));
    ASSERT_TRUE(DBComparator::same<Edge>(e2_1, e2_2));

    delete db1;
    delete db2;
}

TEST_F(ComparatorTest, DBComparison) {
    DB* db1 = cyberSecurityDB();
    DB* db2 = cyberSecurityDB();

    Writeback wb1 {db1};
    Writeback wb2 {db2};
    ASSERT_TRUE(DBComparator::same(db1, db2));

    Network* net1 = db1->getNetwork(db1->getString("cyber-security-db"));
    Network* net2 = db2->getNetwork(db2->getString("cyber-security-db"));
    ASSERT_TRUE(net1);
    ASSERT_TRUE(net2);

    NodeType* nt1 = wb1.createNodeType(db1->getString("NodeType"));
    ASSERT_TRUE(nt1);
    ASSERT_FALSE(DBComparator::same(db1, db2));
    NodeType* nt2 = wb2.createNodeType(db2->getString("NodeType"));
    ASSERT_TRUE(nt2);
    ASSERT_TRUE(DBComparator::same(db1, db2));

    EdgeType* et1 = wb1.createEdgeType(db1->getString("EdgeType"), nt1, nt1);
    ASSERT_TRUE(et1);
    ASSERT_FALSE(DBComparator::same(db1, db2));
    EdgeType* et2 = wb2.createEdgeType(db2->getString("EdgeType"), nt2, nt2);
    ASSERT_TRUE(et2);
    ASSERT_TRUE(DBComparator::same(db1, db2));

    Node* n11 = wb1.createNode(net1, nt1, db1->getString("Node1"));
    Node* n12 = wb1.createNode(net1, nt1, db1->getString("Node2"));
    ASSERT_TRUE(n11);
    ASSERT_TRUE(n12);
    ASSERT_FALSE(DBComparator::same(db1, db2));
    Node* n21 = wb2.createNode(net2, nt2, db2->getString("Node1"));
    Node* n22 = wb2.createNode(net2, nt2, db2->getString("Node2"));
    ASSERT_TRUE(n21);
    ASSERT_TRUE(n22);
    ASSERT_TRUE(DBComparator::same(db1, db2));

    Edge* e1 = wb1.createEdge(et1, n11, n12);
    ASSERT_TRUE(e1);
    ASSERT_FALSE(DBComparator::same(db1, db2));
    Edge* e2 = wb2.createEdge(et2, n21, n22);
    ASSERT_TRUE(e2);
    ASSERT_TRUE(DBComparator::same(db1, db2));

    // The order in which objects are pushed into the database matters1
    // For example:
    // NodeType first then EdgeType
    wb1.createNodeType(db1->getString("NodeType2"));
    wb1.createEdgeType(db1->getString("EdgeType2"), nt1, nt1);
    // EdgeType then NodeType
    wb2.createEdgeType(db2->getString("EdgeType2"), nt2, nt2);
    wb2.createNodeType(db2->getString("NodeType2"));
    ASSERT_FALSE(DBComparator::same(db1, db2));

    delete db1;
    delete db2;
}

TEST_F(ComparatorTest, MultiNetComparison) {
    DB* db1 = getMultiNetNeo4j4DB({"cyber-security-db", "network-db"});
    DB* db2 = getMultiNetNeo4j4DB({"cyber-security-db", "network-db"});
    DB* db3 = getMultiNetNeo4j4DB({"cyber-security-db"});

    ASSERT_TRUE(DBComparator::same(db1, db2));
    ASSERT_FALSE(DBComparator::same(db1, db3));

    delete db1;
    delete db2;
    delete db3;
}
