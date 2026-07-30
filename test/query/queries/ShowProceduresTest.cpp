#include <gtest/gtest.h>

#include "TuringDB.h"
#include "QueryConfig.h"
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

TEST_F(ShowProceduresTest, showProcedures) {
    query("INSTALL greeter", "default");

    bool executed = false;
    const auto res = query("SHOW PROCEDURES", "default", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 2);
        ASSERT_EQ(df->getLogicalRowCount(), 14);

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
        ASSERT_EQ(colName->at(4), "db.describeCommit");
        ASSERT_EQ(colName->at(5), "db.procedures");
        ASSERT_EQ(colName->at(6), "db.showIndexes");
        ASSERT_EQ(colName->at(7), "db.hierarchicalLabelCounts");
        ASSERT_EQ(colName->at(8), "db.listNodes");
        ASSERT_EQ(colName->at(9), "db.getEdges");
        ASSERT_EQ(colName->at(10), "db.getNodes");
        ASSERT_EQ(colName->at(11), "db.getNodeEdges");
        ASSERT_EQ(colName->at(12), "gnn.neighbourhoodSample");
        ASSERT_EQ(colName->at(13), "greeter.hello");

        // Check exact signatures
        ASSERT_EQ(colSignature->at(0), "db.labels() :: (id :: INTEGER, label :: STRING)");
        ASSERT_EQ(colSignature->at(1),
                  "db.propertyTypes() :: (id :: INTEGER, propertyType :: STRING, valueType :: STRING)");
        ASSERT_EQ(colSignature->at(2), "db.edgeTypes() :: (id :: INTEGER, edgeType :: STRING)");
        ASSERT_EQ(colSignature->at(3),
                  "db.history() :: (commit :: STRING, nodeCount :: INTEGER, edgeCount :: INTEGER, "
                  "partCount :: INTEGER)");
        ASSERT_EQ(colSignature->at(4), "db.describeCommit(commit :: STRING)"
                                       " :: (nodeCount :: INTEGER, edgeCount :: INTEGER, partCount :: INTEGER)");
        ASSERT_EQ(colSignature->at(5), "db.procedures() :: (name :: STRING, signature :: STRING)");
        ASSERT_EQ(colSignature->at(6), "db.showIndexes() :: (name :: STRING, size :: INTEGER)");
        ASSERT_EQ(colSignature->at(7), "db.hierarchicalLabelCounts(currentLabels :: LIST) :: (label :: STRING, nodeCount :: INTEGER)");
        ASSERT_EQ(colSignature->at(8),
                  "db.listNodes(labels :: LIST, propertyKeys :: LIST, propertyValues :: LIST, "
                  "skip :: INTEGER, limit :: INTEGER) :: (id :: NODE, labels :: LIST, properties :: STRING)");
        ASSERT_EQ(colSignature->at(9),
                  "db.getEdges(edgeIDs :: LIST) :: (id :: EDGE, src :: NODE, tgt :: NODE, "
                  "edgeTypeID :: INTEGER, properties :: STRING)");
        ASSERT_EQ(colSignature->at(10),
                  "db.getNodes(nodeIDs :: LIST) :: (id :: NODE, labels :: LIST, "
                  "inEdgeCount :: INTEGER, outEdgeCount :: INTEGER, properties :: STRING)");
        ASSERT_EQ(colSignature->at(11),
                  "db.getNodeEdges(nodeIDs :: LIST, defaultLimit :: INTEGER, outLimitTypes :: LIST, "
                  "outLimitValues :: LIST, inLimitTypes :: LIST, inLimitValues :: LIST, "
                  "returnOnlyIDs :: BOOLEAN) :: (id :: NODE, outgoingEdges :: LIST, "
                  "incomingEdges :: LIST, outEdgeCounts :: STRING, inEdgeCounts :: STRING)");
        ASSERT_EQ(colSignature->at(12),
                  "gnn.neighbourhoodSample(node :: NODE, sampleSize :: INTEGER, seed :: INTEGER = null)"
                  " :: (src :: NODE, edge :: EDGE, edgeType :: INTEGER, dst :: NODE)");
        ASSERT_EQ(colSignature->at(13), "greeter.hello() :: (message :: STRING)");

        executed = true;
    });

    spdlog::info(res.getError());
    ASSERT_TRUE(res.isOk());
    ASSERT_TRUE(executed);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
