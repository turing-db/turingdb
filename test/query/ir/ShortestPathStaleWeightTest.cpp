#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class ChangeIDSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        const ColumnVector<ChangeID>* changes = static_cast<const ColumnVector<ChangeID>*>(chunks.front());

        for (size_t row = offset; row < offset + rowCount; row++) {
            _changeID = (*changes)[row];
        }
    }

    ChangeID getChangeID() const { return _changeID; }

private:
    ChangeID _changeID;
};

class DiscardSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

}

// One road, whose weight a first change writes at 1.0 and a second one overwrites at 5.0,
// so the two dataparts disagree on it.
class ShortestPathStaleWeightTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            _graph = system.createGraph(_graphName);
        }

        applyWrite("CREATE (a:City {id: 'a'})-[:ROAD {distance: 1.0}]->(b:City {id: 'b'})");
    }

    void execute(std::string_view query, const ChangeID& changeID, NLOutputSink* sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              sink);
    }

    void openChange(ChangeID& changeID) {
        ChangeIDSink sink;
        QueryStatus status;
        execute("CHANGE NEW", ChangeID::head(), &sink, status);
        ASSERT_TRUE(status.isOk()) << status.getError();

        changeID = sink.getChangeID();
    }

    void applyWrite(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        DiscardSink sink;

        QueryStatus writeStatus;
        execute(query, changeID, &sink, writeStatus);
        ASSERT_TRUE(writeStatus.isOk()) << "query: " << query << "\nerror: " << writeStatus.getError();

        QueryStatus commitStatus;
        execute("COMMIT", changeID, &sink, commitStatus);
        ASSERT_TRUE(commitStatus.isOk()) << commitStatus.getError();

        QueryStatus submitStatus;
        execute("CHANGE SUBMIT", changeID, &sink, submitStatus);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    void read(std::string_view query, std::vector<StringRowSink::Row>& rows) {
        StringRowSink sink;
        QueryStatus status;
        execute(query, ChangeID::head(), &sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        sink.sortedRows(rows);
    }

    size_t getDataPartCount() {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        return view.dataparts().size();
    }

    const std::string _graphName = "roaddb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    Graph* _graph {nullptr};
};

TEST_F(ShortestPathStaleWeightTest, readsTheWeightCommittedByTheNewerDatapart) {
    applyWrite("MATCH (a:City {id: 'a'})-[r:ROAD]->(b:City {id: 'b'}) SET r.distance = 5.0");

    ASSERT_EQ(getDataPartCount(), 2u);

    std::vector<StringRowSink::Row> weight;
    read("MATCH (a:City)-[r:ROAD]->(b:City) RETURN r.distance", weight);

    const std::vector<StringRowSink::Row> expectedWeight {{"5"}};
    ASSERT_EQ(weight, expectedWeight);

    std::vector<StringRowSink::Row> distance;
    read("MATCH (a:City {id: 'a'}), (b:City {id: 'b'}) "
         "SHORTESTPATH(a, b, distance, d, p) RETURN d",
         distance);

    const std::vector<StringRowSink::Row> expectedDistance {{"5"}};
    EXPECT_EQ(distance, expectedDistance) << "shortest path costed the road at the value the older datapart holds";
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
