#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

std::string renderList(ListView list);

// One element of a list as its value, recursing into a nested list.
std::string renderElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<types::Int64::Primitive>());
        break;
        case ListBufferTypeTag::Bool:
            return static_cast<bool>(element.getAs<types::Bool::Primitive>()) ? "true" : "false";
        break;
        case ListBufferTypeTag::String:
            return std::string(element.getAs<types::String::Primitive>());
        break;
        case ListBufferTypeTag::ListView:
            return renderList(element.getAs<ListView>());
        break;
        case ListBufferTypeTag::Null:
            return "null";
        break;
        default:
            return "?";
        break;
    }
}

std::string renderList(const ListView list) {
    std::string rendered = "[";

    for (size_t index = 0; const ListElementView element : list) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += renderElement(element);
        index++;
    }

    return rendered + "]";
}

}

// List properties through the v2 pipeline - the engine TuringDB::query runs - so a list
// written by one query is read back out of the datapart by the next, as a nullable list
// column rather than the column the writing query built.
class ListPropertyQueriesTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph("default");
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    void newChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        _currentChange = res.value()->id();
    }

    QueryStatus query(std::string_view text, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);

        const QueryState state(_graphName,
                               &_env->getMem(),
                               &_queryConfig,
                               &callbacks,
                               CommitHash::head(),
                               _currentChange);
        return _db->query(text, state);
    }

    void write(std::string_view text) {
        newChange();

        const QueryStatus writeStatus = query(text, [](const Dataframe*) {});
        ASSERT_TRUE(writeStatus) << text << ": " << writeStatus.getError();

        const QueryStatus submitStatus = query("change submit", [](const Dataframe*) {});
        ASSERT_TRUE(submitStatus) << submitStatus.getError();
        _currentChange = ChangeID::head();
    }

    void readLists(std::string_view text, std::vector<std::string>& lists) {
        lists.clear();

        const QueryStatus status = query(text, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);

            const auto* column = df->cols().front()->as<ColumnOptVector<ListView>>();
            ASSERT_TRUE(column) << "expected a nullable list column";

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t row = 0; row < rowCount; row++) {
                const std::optional<ListView>& list = column->at(row);
                lists.push_back(list ? renderList(*list) : "null");
            }
        });

        ASSERT_TRUE(status) << text << ": " << status.getError();
    }

    std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
    ChangeID _currentChange {ChangeID::head()};
};

TEST_F(ListPropertyQueriesTest, readsBackAStoredList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");

    std::vector<std::string> lists;
    readLists("MATCH (n:Tagged) RETURN n.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1, 2, 3]"}));
}

TEST_F(ListPropertyQueriesTest, readsBackAHeterogeneousList) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 'two', true, null]})");

    std::vector<std::string> lists;
    readLists("MATCH (n:Tagged) RETURN n.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1, two, true, null]"}));
}

TEST_F(ListPropertyQueriesTest, readsBackANestedList) {
    write("CREATE (n:Tagged {name: 'a', tags: [[1, 2], [3]]})");

    std::vector<std::string> lists;
    readLists("MATCH (n:Tagged) RETURN n.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[[1, 2], [3]]"}));
}

TEST_F(ListPropertyQueriesTest, readsNullWhereTheNodeHasNoList) {
    write("CREATE (a:Tagged {name: 'a', tags: [1]})");
    write("CREATE (b:Tagged {name: 'b'})");

    std::vector<std::string> lists;
    readLists("MATCH (n:Tagged) RETURN n.tags", lists);

    ASSERT_EQ(lists.size(), 2u);
    EXPECT_EQ(lists[0], "[1]");
    EXPECT_EQ(lists[1], "null");
}

TEST_F(ListPropertyQueriesTest, readsBackAListSetOnAMatchedNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.tags = [7, 8]");

    std::vector<std::string> lists;
    readLists("MATCH (n:Person {name: 'Remy'}) RETURN n.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[7, 8]"}));
}

TEST_F(ListPropertyQueriesTest, readsBackAListStoredOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[e:TAGGED {tags: [1, 2]}]->(b)");

    std::vector<std::string> lists;
    readLists("MATCH (:Person)-[e:TAGGED]->(:Person) RETURN e.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1, 2]"}));
}

TEST_F(ListPropertyQueriesTest, readsAListAcrossACartesianProduct) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");

    // The one list is replicated over the rows the product makes, so the view each row
    // carries still names elements the datapart owns.
    std::vector<std::string> lists;
    readLists("MATCH (n:Tagged), (m:Person) RETURN n.tags", lists);

    ASSERT_FALSE(lists.empty());
    EXPECT_EQ(lists, std::vector<std::string>(lists.size(), "[1, 2]"));
}
