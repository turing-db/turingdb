#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<std::string>;

std::string renderElement(ListElementView element);

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

std::string renderElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::Double:
            return std::to_string(element.getAs<double>());
        break;

        case ListBufferTypeTag::Bool:
            return static_cast<bool>(element.getAs<CustomBool>()) ? "true" : "false";
        break;

        case ListBufferTypeTag::String:
            return std::string(element.getAs<std::string_view>());
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

}

// UNWIND of a literal list through the query engine the server runs by default, the
// counterpart of CypherUnwindTest on the db dialect path.
class UnwindQueriesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _db = &_env->getDB();
    }

protected:
    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe);
            ASSERT_EQ(dataframe->cols().size(), 1);

            const NamedColumn* named = dataframe->cols().front();
            const auto* elements = named->as<ColumnVector<ListElementView>>();
            ASSERT_TRUE(elements);

            for (const ListElementView element : *elements) {
                actual.push_back(renderElement(element));
            }
        });

        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);

        ASSERT_TRUE(status) << QueryStatusDescription::value(status.getStatus()) << ": " << status.getError();
        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(UnwindQueriesTest, unwindsListMixingScalarsWithNestedListAndNull) {
    const Rows expected = {"1", "true", "hello", std::to_string(3.14), "[2]", "null"};
    expectRows("UNWIND [1, true, 'hello', 3.14, [2], null] AS l RETURN l", expected);
}

TEST_F(UnwindQueriesTest, unwindsSingletonNullList) {
    const Rows expected = {"null"};
    expectRows("UNWIND [null] AS l RETURN l", expected);
}

TEST_F(UnwindQueriesTest, sortsHeterogeneousUnwind) {
    // Cells of different types compare by the order their types sort in, following
    // Cypher's orderability: LIST < STRING < BOOLEAN < NUMBER.
    const Rows expected = {"hello", "true", "1", std::to_string(3.14)};
    expectRows("UNWIND [1, true, 3.14, 'hello'] AS l RETURN l ORDER BY l", expected);
}

TEST_F(UnwindQueriesTest, sortsHeterogeneousUnwindWithNullsLast) {
    const Rows expected = {"[2]", "a", "1", "null"};
    expectRows("UNWIND [1, null, 'a', [2]] AS l RETURN l ORDER BY l", expected);
}
