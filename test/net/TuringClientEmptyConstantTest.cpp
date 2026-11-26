#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "CellView.h"
#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "columns/ColumnConst.h"
#include "dataframe/Dataframe.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t PERSON_COUNT = 8;

constexpr std::string_view NO_PERSON = "MATCH (n:Person) WHERE n.name = 'zzz' ";

struct ExpectedColumn {
    std::string_view _name;
    ColumnKind::Code _kind {0};
};

struct ReceivedColumn {
    std::string _name;
    ColumnKind::Code _kind {0};
};

struct ReceivedResult {
    size_t _frameCount {0};
    size_t _rowCount {0};
    std::vector<ReceivedColumn> _columns;
};

template <typename T>
constexpr ColumnKind::Code constantOf() {
    return ColumnConst<T>::staticKind();
}

}

// The C++ TuringClient decodes a constant column into a db::ColumnConst, whose value a
// consumer reads once per row. A response with no rows declares its constant columns and
// sends no value for them, so a consumer must find no row to read it in.
class TuringClientEmptyConstantTest : public ProtoServerTest {
protected:
    void initialize() override {
        ProtoServerTest::initialize();

        _client = std::make_unique<net::proto::TuringClient>("127.0.0.1", std::to_string(_port), &_env->getMem());
        _client->setGraphName("simpledb");
        _client->connect();
    }

    void terminate() override {
        _client->disconnect();
        ProtoServerTest::terminate();
    }

    void receive(const std::string& query, ReceivedResult& result) {
        const QueryStatus status = _client->sendQuery(query, [&result](const Dataframe* dataframe) {
            result._frameCount++;
            result._rowCount += dataframe->getLogicalRowCount();
            result._columns.clear();

            for (const NamedColumn* namedColumn : dataframe->cols()) {
                const Column* column = namedColumn->getColumn();
                result._columns.push_back(ReceivedColumn {
                    ._name = std::string(namedColumn->getName()),
                    ._kind = column->getKind(),
                });
            }
        });

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void expectEmptyConstants(const std::string& query, const std::vector<ExpectedColumn>& expected) {
        ReceivedResult result;
        ASSERT_NO_FATAL_FAILURE(receive(query, result));

        EXPECT_EQ(result._frameCount, 1u) << query;
        EXPECT_EQ(result._rowCount, 0u) << query;
        ASSERT_EQ(result._columns.size(), expected.size()) << query;

        for (size_t index = 0; index < expected.size(); index++) {
            EXPECT_EQ(result._columns[index]._name, expected[index]._name) << query;
            EXPECT_EQ(result._columns[index]._kind, expected[index]._kind)
                << query << ": column " << expected[index]._name << " is not the constant column of its type";
        }

        ResultRows rows;
        QueryStatus status;
        runProtoQuery(_client.get(), query, nullptr, rows, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_TRUE(rows.empty()) << query;
    }

    std::unique_ptr<net::proto::TuringClient> _client;
};

TEST_F(TuringClientEmptyConstantTest, cellConstantOverNoMatchedRow) {
    expectEmptyConstants(std::string(NO_PERSON) + "RETURN [1, 'a'][0]",
                         {{"[1, 'a'][0]", constantOf<CellView>()}});
}

TEST_F(TuringClientEmptyConstantTest, cellConstantOverAnEmptyUnwind) {
    expectEmptyConstants("UNWIND [] AS x RETURN [1, 'a'][0] AS c, {x: 1} AS m",
                         {{"c", constantOf<CellView>()}, {"m", constantOf<MapView>()}});
}

TEST_F(TuringClientEmptyConstantTest, everyConstantKindOverNoMatchedRow) {
    const std::string query = std::string(NO_PERSON)
                            + "RETURN 1 AS i, 'a' AS s, 1.5 AS d, true AS b, "
                              "[1, 'a'] AS l, {x: 1} AS m, [1, 'a'][0] AS c, (1.0, 2.0) AS e, null AS z";

    expectEmptyConstants(query,
                         {
                             {"i", constantOf<types::Int64::Primitive>()},
                             {"s", constantOf<types::String::Primitive>()},
                             {"d", constantOf<types::Double::Primitive>()},
                             {"b", constantOf<types::Bool::Primitive>()},
                             {"l", constantOf<ListView>()},
                             {"m", constantOf<MapView>()},
                             {"c", constantOf<CellView>()},
                             {"e", constantOf<types::Embedding::Primitive>()},
                             {"z", constantOf<PropertyNull>()},
                         });
}

TEST_F(TuringClientEmptyConstantTest, cellConstantOverMatchedRows) {
    const std::string query = "MATCH (n:Person) RETURN [1, 'a'][0] AS c, {x: 1} AS m";

    ResultRows rows;
    QueryStatus status;
    runProtoQuery(_client.get(), query, nullptr, rows, status);
    ASSERT_TRUE(status.isOk()) << status.getError();

    ASSERT_EQ(rows.size(), PERSON_COUNT);
    for (const std::vector<std::string>& row : rows) {
        ASSERT_EQ(row.size(), 2u);
        EXPECT_EQ(row[0], "1");
    }
}

TEST_F(TuringClientEmptyConstantTest, emptyResultThenMatchedRows) {
    expectEmptyConstants(std::string(NO_PERSON) + "RETURN [1, 'a'][0] AS c",
                         {{"c", constantOf<CellView>()}});

    ResultRows rows;
    QueryStatus status;
    runProtoQuery(_client.get(), "MATCH (n:Person) RETURN [1, 'a'][0] AS c", nullptr, rows, status);
    ASSERT_TRUE(status.isOk()) << status.getError();

    ASSERT_EQ(rows.size(), PERSON_COUNT);
    for (const std::vector<std::string>& row : rows) {
        ASSERT_EQ(row.size(), 1u);
        EXPECT_EQ(row[0], "1");
    }
}
