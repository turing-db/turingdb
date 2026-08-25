#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<std::string> sortedLabels {"Bioinformatics",
                                             "Exotic",
                                             "Founder",
                                             "Interest",
                                             "Person",
                                             "Sales",
                                             "SleepDisturber",
                                             "SoftwareEngineering",
                                             "Supernatural"};

void firstColumn(const std::vector<StringRowSink::Row>& rows, std::vector<std::string>& column) {
    column.clear();
    for (const StringRowSink::Row& row : rows) {
        column.push_back(row.front());
    }
}

}

class CallYieldedSortKeyTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CallYieldedSortKeyTest, sortsOnAYieldedString) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label ORDER BY label", sink);

    std::vector<std::string> labels;
    firstColumn(sink.getRows(), labels);
    EXPECT_EQ(labels, sortedLabels);
}

TEST_F(CallYieldedSortKeyTest, sortsOnAYieldedStringDescending) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label ORDER BY label DESC", sink);

    std::vector<std::string> labels;
    firstColumn(sink.getRows(), labels);

    std::vector<std::string> expected = sortedLabels;
    std::reverse(expected.begin(), expected.end());
    EXPECT_EQ(labels, expected);
}

// The label IDs a schema procedure yields are their own kind of column, neither a node
// ID nor a property value, and the sort orders them all the same.
TEST_F(CallYieldedSortKeyTest, sortsOnAYieldedLabelID) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD id, label RETURN id ORDER BY id", sink);

    std::vector<uint64_t> ids;
    for (const StringRowSink::Row& row : sink.getRows()) {
        ids.push_back(std::stoull(row.front()));
    }

    EXPECT_EQ(ids.size(), sortedLabels.size());
    EXPECT_TRUE(std::is_sorted(ids.begin(), ids.end()));
}

// A yielded string grouped on and then sorted: the group rows come back in label order,
// each label counted once.
TEST_F(CallYieldedSortKeyTest, sortsAGroupedYieldedKey) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label, count(*) ORDER BY label", sink);

    std::vector<StringRowSink::Row> expected;
    for (const std::string& label : sortedLabels) {
        expected.push_back({label, "1"});
    }

    EXPECT_EQ(sink.getRows(), expected);
}
