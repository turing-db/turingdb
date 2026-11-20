#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "PySink.h"
#include "PyValueStore.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"

using namespace db;
using namespace pybindings;
using namespace turing::test;

namespace {

bool isNullY(size_t row) {
    const int64_t value = static_cast<int64_t>(row);
    return (value >= 65530 && value < 65545) || value % 3 == 0;
}

// Small enough that strings, lists and maps keep running from one packet into the next
constexpr size_t SERVER_BUFFER_CAPACITY = 512;

constexpr size_t ROW_COUNT = 200000;

// Rows a chunk of a variable-length column held, read before the chunk is dropped
template <typename T, typename Row>
void readChunkRows(PyColumn* column, std::vector<Row>& rows, Row (*read)(const T&)) {
    if (column->getKind() == net::proto::ColumnKind::VECTOR) {
        PyColumnVector<T>* vector = static_cast<PyColumnVector<T>*>(column);
        for (size_t row = 0; row < vector->size(); row++) {
            rows.push_back(read((*vector)[row]));
        }
        vector->clear();
    } else {
        ASSERT_EQ(column->getKind(), net::proto::ColumnKind::OPTIONAL_VECTOR);
        PyColumnOptVector<T>* vector = static_cast<PyColumnOptVector<T>*>(column);
        const T* const values = vector->getChunkValues();
        for (size_t row = 0; row < vector->size(); row++) {
            ASSERT_TRUE(vector->isValid(row));
            rows.push_back(read(values[row]));
        }
        vector->clear();
    }
}

std::string readString(const std::string_view& value) {
    return std::string(value);
}

std::vector<int64_t> readList(const ListView& value) {
    std::vector<int64_t> elements;
    for (const CellView element : value.elements()) {
        elements.push_back(element.getAs<int64_t>());
    }
    return elements;
}

int64_t readMap(const MapView& value) {
    int64_t sum = 0;
    for (const MapEntryView entry : value.entries()) {
        EXPECT_EQ(entry.getKey(), "k");
        sum += entry.getValue().getAs<int64_t>();
    }
    return sum;
}

}

// The Python sink decodes every result chunk of a response into its columns, a fixed-width
// column across chunks into one vector and a variable-length one a chunk at a time
class PySinkDecodeTest : public ProtoServerTest {
protected:
    PySinkDecodeTest() {
        _serverConfig.setProtoBufferCapacity(SERVER_BUFFER_CAPACITY);
    }

    void initialize() override {
        ProtoServerTest::initialize();

        _client = std::make_unique<net::proto::TuringClient>("127.0.0.1", std::to_string(_port), &_env->getMem());
        _client->setGraphName("simpledb");
        _client->connect();

        _sink = std::make_unique<PySink>(&_values);
    }

    void terminate() override {
        _client->disconnect();
        ProtoServerTest::terminate();
    }

    QueryStatus run(const std::string& query, const std::function<void()>& onChunkDecoded) {
        _chunkCount = 0;

        const auto countChunk = [this, &onChunkDecoded]() {
            _chunkCount++;
            onChunkDecoded();
            _values.clearValues();
        };

        return _client->sendQuery(query, _sink.get(), &_container, countChunk);
    }

    PyValueStore _values;
    std::unique_ptr<PySink> _sink;
    PyColumnContainer _container;
    std::unique_ptr<net::proto::TuringClient> _client;
    size_t _chunkCount {0};
};

TEST_F(PySinkDecodeTest, accumulatesNullableIntegersAcrossChunks) {
    const std::string query = "UNWIND range(0, " + std::to_string(ROW_COUNT - 1) + ") AS x "
                              "RETURN x, CASE WHEN (x >= 65530 AND x < 65545) OR x % 3 = 0 THEN null ELSE x END AS y, 7 AS c";

    size_t rowCount = 0;
    const auto onChunkDecoded = [this, &rowCount]() {
        ASSERT_EQ(_container.size(), 3u);

        for (size_t index = 0; index < 2; index++) {
            PyColumn* column = _container[index];
            ASSERT_EQ(column->getKind(), net::proto::ColumnKind::OPTIONAL_VECTOR);
            ASSERT_EQ(column->getInternalType(), net::proto::ColumnInternalKind::INT64);
        }

        PyColumnOptVector<int64_t>* y = static_cast<PyColumnOptVector<int64_t>*>(_container[1]);
        ASSERT_EQ(y->size(), _container.getRowCount());
        for (size_t row = 0; row < y->size(); row++) {
            ASSERT_EQ(y->isValid(row), !isNullY(rowCount + row)) << rowCount + row;
        }

        static_cast<PyColumnOptVector<int64_t>*>(_container[0])->endChunk();
        y->endChunk();

        PyColumn* constant = _container[2];
        ASSERT_EQ(constant->getKind(), net::proto::ColumnKind::CONSTANT);
        static_cast<PyColumnConst<int64_t>*>(constant)->appendRepeated(_container.getRowCount());

        rowCount += _container.getRowCount();
    };

    const QueryStatus status = run(query, onChunkDecoded);
    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_GT(_chunkCount, 2u);
    ASSERT_EQ(rowCount, ROW_COUNT);

    EXPECT_EQ(_container.getName(0), "x");
    EXPECT_EQ(_container.getName(1), "y");
    EXPECT_EQ(_container.getName(2), "c");

    PyColumnOptVector<int64_t>* x = static_cast<PyColumnOptVector<int64_t>*>(_container[0]);
    PyColumnOptVector<int64_t>* y = static_cast<PyColumnOptVector<int64_t>*>(_container[1]);
    PyColumnConst<int64_t>* c = static_cast<PyColumnConst<int64_t>*>(_container[2]);

    ASSERT_EQ(x->getRows().size(), ROW_COUNT);
    ASSERT_EQ(x->getNullMask().size(), ROW_COUNT);
    ASSERT_EQ(y->getRows().size(), ROW_COUNT);
    ASSERT_EQ(y->getNullMask().size(), ROW_COUNT);
    ASSERT_EQ(c->getRows().size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        const int64_t value = static_cast<int64_t>(row);
        const bool yIsNull = isNullY(row);

        ASSERT_EQ(x->getNullMask()[row], 0) << row;
        ASSERT_EQ(x->getRows()[row], value) << row;
        ASSERT_EQ(y->getNullMask()[row], yIsNull ? 1 : 0) << row;
        if (!yIsNull) {
            ASSERT_EQ(y->getRows()[row], value) << row;
        }
        ASSERT_EQ(c->getRows()[row], 7) << row;
    }
}

TEST_F(PySinkDecodeTest, readsStringsListsAndMapsSplitAcrossPackets) {
    const std::string query = "UNWIND range(0, " + std::to_string(ROW_COUNT - 1) + ") AS x "
                              "RETURN 'value_' + toString(x) AS s, [x, x + 1] AS l, {k: x} AS m";

    std::vector<std::string> strings;
    std::vector<std::vector<int64_t>> lists;
    std::vector<int64_t> maps;
    const auto onChunkDecoded = [this, &strings, &lists, &maps]() {
        ASSERT_EQ(_container.size(), 3u);
        ASSERT_EQ(_container[0]->getInternalType(), net::proto::ColumnInternalKind::STRING);
        ASSERT_EQ(_container[1]->getInternalType(), net::proto::ColumnInternalKind::LIST_VIEW);
        ASSERT_EQ(_container[2]->getInternalType(), net::proto::ColumnInternalKind::MAP_VIEW);

        readChunkRows<std::string_view, std::string>(_container[0], strings, readString);
        readChunkRows<ListView, std::vector<int64_t>>(_container[1], lists, readList);
        readChunkRows<MapView, int64_t>(_container[2], maps, readMap);
    };

    const QueryStatus status = run(query, onChunkDecoded);
    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_GT(_chunkCount, 2u);

    ASSERT_EQ(strings.size(), ROW_COUNT);
    ASSERT_EQ(lists.size(), ROW_COUNT);
    ASSERT_EQ(maps.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        const int64_t value = static_cast<int64_t>(row);
        ASSERT_EQ(strings[row], "value_" + std::to_string(row));
        ASSERT_EQ(lists[row], (std::vector<int64_t> {value, value + 1}));
        ASSERT_EQ(maps[row], value);
    }
}

TEST_F(PySinkDecodeTest, readsAStringLongerThanManyPackets) {
    const std::string longText(5 * SERVER_BUFFER_CAPACITY + 17, 'z');
    const std::string query = "UNWIND range(0, 9) AS x RETURN '" + longText + "' + toString(x) AS s";

    std::vector<std::string> strings;
    const auto onChunkDecoded = [this, &strings]() {
        ASSERT_EQ(_container.size(), 1u);
        readChunkRows<std::string_view, std::string>(_container[0], strings, readString);
    };

    const QueryStatus status = run(query, onChunkDecoded);
    ASSERT_TRUE(status.isOk()) << status.getError();

    ASSERT_EQ(strings.size(), 10u);
    for (size_t row = 0; row < strings.size(); row++) {
        EXPECT_EQ(strings[row], longText + std::to_string(row));
    }
}

TEST_F(PySinkDecodeTest, seesTheColumnsOfAnEmptyResult) {
    const QueryStatus status = run("UNWIND range(0, -1) AS x RETURN x, 'a' + toString(x) AS s", [this]() {
        EXPECT_EQ(_container.getRowCount(), 0u);
    });
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(_chunkCount, 1u);
    ASSERT_EQ(_container.size(), 2u);
    EXPECT_EQ(_container.getName(0), "x");
    EXPECT_EQ(_container.getName(1), "s");
}

TEST_F(PySinkDecodeTest, reportsAnErrorAndStaysUsable) {
    const QueryStatus failed = run("MATCH (n) RETURN m", []() {});
    EXPECT_FALSE(failed.isOk());
    EXPECT_FALSE(failed.getError().empty());

    PyColumnContainer container;
    PySink sink(&_values);

    size_t rowCount = 0;
    const auto onChunkDecoded = [&container, &rowCount]() {
        rowCount += container.getRowCount();
        static_cast<PyColumnOptVector<int64_t>*>(container[0])->clear();
    };

    const QueryStatus status = _client->sendQuery("UNWIND range(1, 3) AS x RETURN x", &sink, &container, onChunkDecoded);
    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(rowCount, 3u);
}
