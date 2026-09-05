#include <gtest/gtest.h>

#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "QueryConfig.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

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
        case ListBufferTypeTag::Double:
            return std::to_string(element.getAs<types::Double::Primitive>());
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

constexpr std::string_view listNodes =
    R"({"type":"node","id":"0","labels":["Item"],"properties":{"name":"a","tags":[1,2,3],"mixed":[1,"two",true,null],"nested":[[1],[2,3]]}})"
    "\n"
    R"({"type":"node","id":"1","labels":["Item"],"properties":{"name":"b","tags":[],"mixed":["only"],"nested":[[]]}})"
    "\n"
    R"({"type":"relationship","id":"0","label":"LINKS","properties":{"weights":[1.5,2.5]},"start":0,"end":1})"
    "\n";

}

// A JSON array that is not declared an embedding imports as a list property: the array
// nesting, the element types it mixes and an empty array all survive into storage, and a
// later query reads them back.
class LoadJsonlListTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _db = &_env->getDB();

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph("default");

        const fs::Path path = _env->getConfig().getDataDir() / _fileName;
        std::ofstream file(path.get());
        file << listNodes;
    }

protected:
    QueryStatus query(std::string_view text, std::string_view graphName, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);

        const QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(text, state);
    }

    void load() {
        const QueryStatus status =
            query(fmt::format("LOAD JSONL \"{}\" AS {}", _fileName, _graphName),
                  "default",
                  [](const Dataframe*) {});
        ASSERT_TRUE(status) << status.getError();
    }

    void readLists(std::string_view text, std::vector<std::string>& lists) {
        lists.clear();

        const QueryStatus status = query(text, _graphName, [&](const Dataframe* df) {
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

    const std::string _fileName = "lists.jsonl";
    const std::string _graphName = "lists";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(LoadJsonlListTest, importsAnIntegerArrayAsAList) {
    load();

    std::vector<std::string> lists;
    readLists("MATCH (n:Item) RETURN n.tags", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1, 2, 3]", "[]"}));
}

TEST_F(LoadJsonlListTest, importsAHeterogeneousArrayAsAList) {
    load();

    std::vector<std::string> lists;
    readLists("MATCH (n:Item) RETURN n.mixed", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1, two, true, null]", "[only]"}));
}

TEST_F(LoadJsonlListTest, importsNestedArraysAsNestedLists) {
    load();

    std::vector<std::string> lists;
    readLists("MATCH (n:Item) RETURN n.nested", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[[1], [2, 3]]", "[[]]"}));
}

TEST_F(LoadJsonlListTest, importsAnEdgeArrayAsAList) {
    load();

    std::vector<std::string> lists;
    readLists("MATCH (:Item)-[e:LINKS]->(:Item) RETURN e.weights", lists);

    EXPECT_EQ(lists, (std::vector<std::string> {"[1.500000, 2.500000]"}));
}
