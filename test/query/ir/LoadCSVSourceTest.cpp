#include <gtest/gtest.h>

#include <stddef.h>
#include <fstream>
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

// Three records of three fields, the fixture every positional case reads.
constexpr std::string_view peopleFile = "Alice,30,London\n"
                                        "Bob,25,Paris\n"
                                        "Charlie,35,Berlin\n";

// The same records behind a header line, so a case can name a field either way.
constexpr std::string_view headedFile = "name,age,city\n"
                                        "Alice,30,London\n"
                                        "Bob,25,Paris\n"
                                        "Charlie,35,Berlin\n";

// A quoted field carrying the delimiter, and a record whose field count disagrees with
// the file's - what ON ERROR SKIP drops and ON ERROR FAIL reports.
constexpr std::string_view messyFile = "name,city\n"
                                       "Dave,\"Paris, France\"\n"
                                       "Broken,one,too many\n"
                                       "Frank,Berlin\n";

// More records than one chunk holds, so the load's loop runs several steps and every
// consumer below it sees the file arrive in pieces.
constexpr size_t chunkedRecordCount = 70000;

// Wide enough to number every one of them, so a name sorts by the record it names.
constexpr size_t chunkedNameDigits = 6;

}

// LOAD CSV through the MLIR engine: a source op of its own, whose one column per field
// the query names the rest of the query reads like any other - converted, cut, ordered,
// reduced.
class LoadCSVSourceTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        writeFile("people.csv", peopleFile);
        writeFile("headed.csv", headedFile);
        writeFile("messy.csv", messyFile);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
        file.close();
    }

    // The name the record at @param position carries: zero-padded, so the names sort the
    // way the numbers do and the extreme of an ORDER BY is the last record's.
    static std::string chunkedName(size_t position) {
        const std::string number = std::to_string(position);

        return "n" + std::string(chunkedNameDigits - number.size(), '0') + number;
    }

    // A file of chunkedRecordCount records, each holding its own number and its name, so
    // every record is distinct and both orders are known.
    static std::string chunkedFile() {
        std::string content;
        for (size_t record = 0; record < chunkedRecordCount; record++) {
            content += std::to_string(record);
            content += ',';
            content += chunkedName(record);
            content += '\n';
        }

        return content;
    }

    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectError(std::string_view query, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_FALSE(status.isOk()) << query << " was expected to fail";
        EXPECT_NE(status.getError().find(reason), std::string::npos) << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadCSVSourceTest, readsEveryFieldByPosition) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN row[0] AS name, row[1] AS age, row[2] AS city",
               {{"Alice", "30", "London"}, {"Bob", "25", "Paris"}, {"Charlie", "35", "Berlin"}});
}

TEST_F(LoadCSVSourceTest, readsAFieldByHeader) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row RETURN row.name AS name, row.city AS city",
               {{"Alice", "London"}, {"Bob", "Paris"}, {"Charlie", "Berlin"}});
}

// The header line names the fields; it does not stop a query from reaching one by the
// position it sits at.
TEST_F(LoadCSVSourceTest, readsAHeadedFileByPosition) {
    expectRows("LOAD CSV 'headed.csv' WITH HEADERS AS row RETURN row[2] AS city",
               {{"London"}, {"Paris"}, {"Berlin"}});
}

// A field named twice is loaded once: both accesses read the one column the load
// publishes for it, which is what makes them the same value.
TEST_F(LoadCSVSourceTest, readsAFieldNamedTwiceOnce) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN row[0] AS first, row[0] AS again",
               {{"Alice", "Alice"}, {"Bob", "Bob"}, {"Charlie", "Charlie"}});
}

// An unaliased item is named after the text the query wrote, as every other projected
// expression is.
TEST_F(LoadCSVSourceTest, namesAnUnaliasedFieldAfterItsText) {
    QueryStatus status;
    StringRowSink sink;
    runQuery("LOAD CSV 'people.csv' AS row RETURN row[1]", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<std::string> expected {"row[1]"};
    EXPECT_EQ(sink.getNames(), expected);
}

// A field carries the characters as written, so the query converts them itself.
TEST_F(LoadCSVSourceTest, convertsAFieldToAnInteger) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN toInteger(row[1]) + 1 AS next",
               {{"31"}, {"26"}, {"36"}});
}

TEST_F(LoadCSVSourceTest, cutsTheRecordsWithSkipAndLimit) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN row[0] AS name SKIP 1 LIMIT 1", {{"Bob"}});
}

TEST_F(LoadCSVSourceTest, ordersTheRecordsByAField) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN row[0] AS name ORDER BY row[0] DESC",
               {{"Charlie"}, {"Bob"}, {"Alice"}});
}

TEST_F(LoadCSVSourceTest, filtersTheRecordsThroughAWith) {
    expectRows("LOAD CSV 'people.csv' AS row WITH row[0] AS name, toInteger(row[1]) AS age "
               "WHERE age > 26 RETURN name, age ORDER BY name",
               {{"Alice", "30"}, {"Charlie", "35"}});
}

TEST_F(LoadCSVSourceTest, reducesTheRecordsToAnAggregate) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN max(toInteger(row[1])) AS oldest", {{"35"}});
}

// A query naming no field still runs over the records, so the tally is the file's record
// count rather than one row.
TEST_F(LoadCSVSourceTest, countsTheRecordsWithoutNamingAField) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN count(*) AS records", {{"3"}});
}

TEST_F(LoadCSVSourceTest, groupsTheRecordsByAField) {
    expectRows("LOAD CSV 'people.csv' AS row RETURN row[2] AS city, count(*) AS n ORDER BY city",
               {{"Berlin", "1"}, {"London", "1"}, {"Paris", "1"}});
}

// A quoted field keeps the delimiter it carries, per RFC 4180.
TEST_F(LoadCSVSourceTest, keepsTheDelimiterAQuotedFieldCarries) {
    expectRows("LOAD CSV 'messy.csv' WITH HEADERS ON ERROR SKIP AS row RETURN row.city AS city",
               {{"Paris, France"}, {"Berlin"}});
}

// ON ERROR FAIL is the default, so a record whose field count disagrees with the file's
// fails the query and names the line it was on.
TEST_F(LoadCSVSourceTest, reportsARecordOfTheWrongFieldCount) {
    expectError("LOAD CSV 'messy.csv' WITH HEADERS AS row RETURN row.city", "CSV line 2");
    expectError("LOAD CSV 'messy.csv' WITH HEADERS ON ERROR FAIL AS row RETURN row.city", "CSV line 2");
}

// The records arrive in chunks, so a consumer of the load sees several steps of its loop
// rather than one; the reduction over them is still the whole file's.
TEST_F(LoadCSVSourceTest, readsAFileSpanningSeveralChunks) {
    writeFile("chunked.csv", chunkedFile());

    expectRows("LOAD CSV 'chunked.csv' AS row RETURN count(*) AS records",
               {{std::to_string(chunkedRecordCount)}});
    expectRows("LOAD CSV 'chunked.csv' AS row RETURN max(toInteger(row[0])) AS last",
               {{std::to_string(chunkedRecordCount - 1)}});
}

// A field owns its characters and the chunk is cleared for the next records, so anything
// buffering rows across steps - a sort, a seen-set, a collect - has to keep a copy of its
// own rather than a view into the chunk it read.
TEST_F(LoadCSVSourceTest, buffersFieldsAcrossChunkBoundaries) {
    writeFile("names.csv", chunkedFile());

    expectRows("LOAD CSV 'names.csv' AS row RETURN row[1] AS name ORDER BY row[1] DESC LIMIT 1",
               {{chunkedName(chunkedRecordCount - 1)}});
    expectRows("LOAD CSV 'names.csv' AS row RETURN count(DISTINCT row[1]) AS distinctNames",
               {{std::to_string(chunkedRecordCount)}});
}

// A LIMIT stops the loop, so the file is read only as far as the budget reaches.
TEST_F(LoadCSVSourceTest, stopsReadingOnceALimitIsSpent) {
    writeFile("chunked.csv", chunkedFile());

    expectRows("LOAD CSV 'chunked.csv' AS row RETURN row[0] AS id LIMIT 2", {{"0"}, {"1"}});
}

// A file holding no record names no field, so there is nothing to resolve a position
// against - and no row to report either.
TEST_F(LoadCSVSourceTest, readsAnEmptyFileAsNoRecord) {
    writeFile("empty.csv", "");

    expectRows("LOAD CSV 'empty.csv' AS row RETURN row[0]", {});
    expectRows("LOAD CSV 'empty.csv' AS row RETURN count(*) AS records", {{"0"}});
}

// A file of nothing but a header line holds no record either.
TEST_F(LoadCSVSourceTest, readsAHeaderOnlyFileAsNoRecord) {
    writeFile("header_only.csv", "name,city\n");

    expectRows("LOAD CSV 'header_only.csv' WITH HEADERS AS row RETURN row.name", {});
}

// Two loads read files that have nothing to do with each other, so their records are
// paired: the cartesian product two patterns naming no common variable produce.
TEST_F(LoadCSVSourceTest, pairsTheRecordsOfTwoLoads) {
    writeFile("two.csv", "x\ny\n");

    expectRows("LOAD CSV 'two.csv' AS a LOAD CSV 'two.csv' AS b RETURN a[0] AS left, b[0] AS right",
               {{"x", "x"}, {"x", "y"}, {"y", "x"}, {"y", "y"}});
}

// The row is not a value: the load publishes one column per field the query names, and
// nothing stands for the whole record.
TEST_F(LoadCSVSourceTest, refusesToReadTheRowAsAWhole) {
    expectError("LOAD CSV 'people.csv' AS row RETURN row", "cannot be read as a whole");
}

// Which field a computed index reads is known no earlier than the row it reads it from,
// so it names no field of the load.
TEST_F(LoadCSVSourceTest, refusesAComputedIndex) {
    expectError("LOAD CSV 'people.csv' AS row RETURN row[1 + 1]", "constant index");
}

// Without a header line the file names no field, so there is nothing for a header access
// to resolve against.
TEST_F(LoadCSVSourceTest, refusesAHeaderAccessOnAHeaderlessLoad) {
    expectError("LOAD CSV 'people.csv' AS row RETURN row.name", "without WITH HEADERS");
}

TEST_F(LoadCSVSourceTest, reportsAHeaderTheFileDoesNotCarry) {
    expectError("LOAD CSV 'headed.csv' WITH HEADERS AS row RETURN row.nope", "header 'nope' not found");
}

TEST_F(LoadCSVSourceTest, reportsAPositionPastTheLastField) {
    expectError("LOAD CSV 'people.csv' AS row RETURN row[7]", "out of range");
}

TEST_F(LoadCSVSourceTest, reportsAMissingFile) {
    expectError("LOAD CSV 'absent.csv' AS row RETURN row[0]", "Cannot open CSV file");
}

// The data directory is the only place a query may read from, so a path reaching out of
// it is refused rather than followed.
TEST_F(LoadCSVSourceTest, refusesAPathOutsideTheDataDirectory) {
    expectError("LOAD CSV '../people.csv' AS row RETURN row[0]", "must be relative to");
}
