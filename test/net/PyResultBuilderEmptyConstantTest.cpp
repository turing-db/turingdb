#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <nanobind/nanobind.h>

#include "EmbeddedPython.h"
#include "ProtoServerTest.h"
#include "PyColumnContainer.h"
#include "PyResultBuilder.h"
#include "PySink.h"
#include "PyValueStore.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "TuringProtoHeaders.h"

using namespace pybindings;
using namespace turing::test;

namespace {

constexpr size_t PERSON_COUNT = 8;

constexpr std::string_view NO_PERSON = "MATCH (n:Person) WHERE n.name = 'zzz' ";

struct ExpectedColumn {
    std::string_view _name;
    net::proto::ColumnInternalKind _internalType {};
    net::proto::ColumnKind _kind {};
};

constexpr ExpectedColumn cellConstant(std::string_view name) {
    return ExpectedColumn {
        ._name = name,
        ._internalType = net::proto::ColumnInternalKind::CELL_VIEW,
        ._kind = net::proto::ColumnKind::CONSTANT,
    };
}

}

// The Python client's result builder, run in an embedded interpreter on the chunks the
// PySink decodes, as PyTuringClient::query runs it. Reading a chunk into Python objects
// needs no numpy, so the cases run wherever the extension builds.
class PyResultBuilderEmptyConstantTest : public ProtoServerTest {
protected:
    static void SetUpTestSuite() {
        initializeEmbeddedPython();
    }

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

    // Runs the query as PyTuringClient::query does, up to the build of the envelope, which
    // needs numpy. Checks the columns while the sink that owns them lives.
    void run(const std::string& query, const std::vector<ExpectedColumn>& expected, size_t& rowCount) {
        PyValueStore values;
        PySink sink(&values);
        PyColumnContainer container;
        PyResultBuilder builder(&container, &values);

        size_t chunkCount = 0;
        rowCount = 0;
        const auto onChunkDecoded = [&values, &builder, &container, &chunkCount, &rowCount]() {
            chunkCount++;
            rowCount += container.getRowCount();
            builder.appendChunk();
            values.clearValues();
        };

        db::QueryStatus status;
        {
            nb::gil_scoped_release release;
            status = _client->sendQuery(query, &sink, &container, onChunkDecoded);
        }

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        ASSERT_GT(chunkCount, 0u) << query;

        ASSERT_EQ(container.size(), expected.size()) << query;
        for (size_t index = 0; index < expected.size(); index++) {
            PyColumn* const column = container[index];
            EXPECT_EQ(container.getName(index), expected[index]._name) << query;
            EXPECT_EQ(column->getInternalType(), expected[index]._internalType) << query << ": column " << expected[index]._name;
            EXPECT_EQ(column->getKind(), expected[index]._kind) << query << ": column " << expected[index]._name;
        }
    }

    void expectRows(const std::string& query, const std::vector<ExpectedColumn>& expected, size_t expectedRowCount) {
        size_t rowCount = 0;
        ASSERT_NO_FATAL_FAILURE(run(query, expected, rowCount));
        EXPECT_EQ(rowCount, expectedRowCount) << query;
    }

    std::unique_ptr<net::proto::TuringClient> _client;
};

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN [1, 'a'][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantOverAnAbsentLabel) {
    expectRows("MATCH (n:Tagged) RETURN [1, 'a'][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantHoldingAMapOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN [{x: 1}][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantHoldingAListOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN [[1, 'a'], 2][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellOfAnUnknownPropertyOverAnAbsentLabel) {
    expectRows("MATCH (n:Tagged) RETURN [n.attrs][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, elementOfAPropertyListOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN [n.name][0] AS c",
               {{"c", net::proto::ColumnInternalKind::STRING, net::proto::ColumnKind::OPTIONAL_VECTOR}},
               0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantBesideAPropertyOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN n.name AS name, [1, 'a'][0] AS c",
               {
                   {"name", net::proto::ColumnInternalKind::STRING, net::proto::ColumnKind::OPTIONAL_VECTOR},
                   cellConstant("c"),
               },
               0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantThroughWithOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "WITH [1, 'a'][0] AS c RETURN c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantOverAnEmptyUnwind) {
    expectRows("UNWIND [] AS x RETURN [1, 'a'][0] AS c", {cellConstant("c")}, 0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellUnderLimitZero) {
    expectRows("MATCH (n:Person) RETURN [1, 'a'][0] AS c LIMIT 0",
               {{"c", net::proto::ColumnInternalKind::CELL_VIEW, net::proto::ColumnKind::VECTOR}},
               0);
}

TEST_F(PyResultBuilderEmptyConstantTest, everyObjectConstantOverNoMatchedRow) {
    expectRows(std::string(NO_PERSON) + "RETURN 'a' AS s, [1, 'a'] AS l, {x: 1} AS m, [1, 'a'][0] AS c, "
                                        "(1.0, 2.0) AS e, null AS z",
               {
                   {"s", net::proto::ColumnInternalKind::STRING, net::proto::ColumnKind::CONSTANT},
                   {"l", net::proto::ColumnInternalKind::LIST_VIEW, net::proto::ColumnKind::CONSTANT},
                   {"m", net::proto::ColumnInternalKind::MAP_VIEW, net::proto::ColumnKind::CONSTANT},
                   cellConstant("c"),
                   {"e", net::proto::ColumnInternalKind::EMBEDDING, net::proto::ColumnKind::CONSTANT},
                   {"z", net::proto::ColumnInternalKind::PROPERTY_NULL, net::proto::ColumnKind::CONSTANT},
               },
               0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellVectorOverAnEmptyUnwind) {
    expectRows("UNWIND [] AS x RETURN CASE WHEN x THEN 1 ELSE 'a' END AS c",
               {{"c", net::proto::ColumnInternalKind::CELL_VIEW, net::proto::ColumnKind::VECTOR}},
               0);
}

TEST_F(PyResultBuilderEmptyConstantTest, cellConstantOverMatchedRows) {
    expectRows("MATCH (n:Person) RETURN [1, 'a'][0] AS c, {x: 1} AS m",
               {cellConstant("c"), {"m", net::proto::ColumnInternalKind::MAP_VIEW, net::proto::ColumnKind::CONSTANT}},
               PERSON_COUNT);
}

TEST_F(PyResultBuilderEmptyConstantTest, emptyResultThenMatchedRows) {
    expectRows(std::string(NO_PERSON) + "RETURN [1, 'a'][0] AS c", {cellConstant("c")}, 0);
    expectRows("MATCH (n:Person) RETURN [1, 'a'][0] AS c", {cellConstant("c")}, PERSON_COUNT);
}
