#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Reads one list cell as a vector of doubles, asserting each element carries the double
// tag: the tag is what tells a renderer a number apart from an entity id.
void readDoubleList(const ListView& view, std::vector<double>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::Double);
        out.push_back(element.getAs<types::Double::Primitive>());
    }
}

// Reads one list cell as a vector of booleans, asserting the boolean tag.
void readBoolList(const ListView& view, std::vector<bool>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::Bool);
        out.push_back(static_cast<bool>(element.getAs<types::Bool::Primitive>()));
    }
}

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Collects the (team, [values]) rows a grouped collect of this element type emits.
template <typename Element, void (*Reader)(const ListView&, std::vector<Element>&)>
class KeyedListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<Element>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            std::vector<Element> values;
            Reader(listRaw[row], values);

            _rows.push_back({key, values});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the single [values] row an ungrouped collect of this element type emits.
template <typename Element, void (*Reader)(const ListView&, std::vector<Element>&)>
class ListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<Element> values;
            Reader(listRaw[row], values);

            _rows.push_back(values);
        }
    }

    const std::vector<std::vector<Element>>& rows() const { return _rows; }

private:
    std::vector<std::vector<Element>> _rows;
};

using KeyedDoubleListSink = KeyedListSink<double, readDoubleList>;
using KeyedBoolListSink = KeyedListSink<bool, readBoolList>;
using DoubleListSink = ListSink<double, readDoubleList>;
using BoolListSink = ListSink<bool, readBoolList>;

}

// collect() over the two scalar element types no other suite gathers - a Double and a
// Bool property - plain and DISTINCT, grouped and ungrouped. Each element type has a fold
// and a list emit of its own selected off the column's value type.
class CypherCollectElementTypeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interp3 = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph(_graphName);
    }

    // Runs a CREATE query in its own change and submits it.
    void create(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink discardSink;
        QueryStatus createStatus;
        _interp3->execute(createStatus, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &discardSink);
        ASSERT_TRUE(createStatus.isOk()) << "CREATE failed: " << createStatus.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});
        const QueryState submitState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    // Runs a read-only MATCH query against the committed head.
    void match(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interp3->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << "MATCH failed: " << status.getError();
    }

    // Inserts 6 nodes over three teams: red repeats a weight and a flag so DISTINCT has
    // something to drop, blue carries one of each beside a node holding neither, and grey
    // holds neither - the group whose every value is null.
    void buildSampleGraph() {
        create(R"(CREATE (:Node {team: "red", name: "alice", weight: 1.5, flag: true}), (:Node {team: "red", name: "carol", weight: 2.5, flag: false}), (:Node {team: "red", name: "erin", weight: 1.5, flag: true}))");
        create(R"(CREATE (:Node {team: "blue", name: "bob", weight: 3.5, flag: false}), (:Node {team: "blue", name: "dan"}), (:Node {team: "grey", name: "gus"}))");
    }

    const std::string _graphName = "sampleGraph";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(CypherCollectElementTypeTest, groupedCollectOfDoubles) {
    buildSampleGraph();

    KeyedDoubleListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.weight)", sink);

    std::vector<KeyedDoubleListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedDoubleListSink::Row> expected {
        {"blue", {3.5}},
        {"grey", {}},
        {"red", {1.5, 2.5, 1.5}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectElementTypeTest, groupedCollectOfDistinctDoubles) {
    buildSampleGraph();

    KeyedDoubleListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(DISTINCT n.weight)", sink);

    std::vector<KeyedDoubleListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedDoubleListSink::Row> expected {
        {"blue", {3.5}},
        {"grey", {}},
        {"red", {1.5, 2.5}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectElementTypeTest, ungroupedCollectOfDoubles) {
    buildSampleGraph();

    DoubleListSink sink;
    match("MATCH (n:Node) RETURN collect(n.weight)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<double> {1.5, 2.5, 1.5, 3.5}));
}

TEST_F(CypherCollectElementTypeTest, ungroupedCollectOfDistinctDoubles) {
    buildSampleGraph();

    DoubleListSink sink;
    match("MATCH (n:Node) RETURN collect(DISTINCT n.weight)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<double> {1.5, 2.5, 3.5}));
}

TEST_F(CypherCollectElementTypeTest, groupedCollectOfBooleans) {
    buildSampleGraph();

    KeyedBoolListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.flag)", sink);

    std::vector<KeyedBoolListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedBoolListSink::Row> expected {
        {"blue", {false}},
        {"grey", {}},
        {"red", {true, false, true}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectElementTypeTest, groupedCollectOfDistinctBooleans) {
    buildSampleGraph();

    KeyedBoolListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(DISTINCT n.flag)", sink);

    std::vector<KeyedBoolListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedBoolListSink::Row> expected {
        {"blue", {false}},
        {"grey", {}},
        {"red", {true, false}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectElementTypeTest, ungroupedCollectOfBooleans) {
    buildSampleGraph();

    BoolListSink sink;
    match("MATCH (n:Node) RETURN collect(n.flag)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<bool> {true, false, true, false}));
}

TEST_F(CypherCollectElementTypeTest, ungroupedCollectOfDistinctBooleans) {
    buildSampleGraph();

    BoolListSink sink;
    match("MATCH (n:Node) RETURN collect(DISTINCT n.flag)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<bool> {true, false}));
}
