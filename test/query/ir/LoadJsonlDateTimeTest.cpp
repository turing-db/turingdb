#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "FileUtils.h"
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

// A datetime is spelled as a string in JSON, and so is a string, so LOAD JSONL is told
// which properties hold instants rather than guessing from what the text looks like. The
// clause is WITH DATETIMES [...], the sibling of WITH EMBEDDINGS, which exists for the
// same reason on the ambiguity between a list and an embedding.
class LoadJsonlDateTimeTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    // LOAD JSONL resolves its path under the data directory, so the fixture is written
    // there rather than beside the test
    void writeFixture(std::string_view fileName, std::string_view contents) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};

        std::ofstream file(dataDir / std::string {fileName});
        ASSERT_TRUE(file.is_open());

        file << contents;
    }

    void runQuery(std::string_view query, std::string_view graphName, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void runQueryExpectingError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectSortedRows(std::string_view query,
                          std::string_view graphName,
                          const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, graphName, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadJsonlDateTimeTest, ReadsANamedPropertyAsAnInstant) {
    writeFixture("events.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z"}}
{"type":"node","id":"1","labels":["Event"],"properties":{"created":"2024-03-14T11:30:00+02:00"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "events.jsonl" AS events WITH DATETIMES ["created"])", _sessionGraph, loadSink);

    // Both rows name the same instant, the second written against an offset
    expectSortedRows("MATCH (n:Event) RETURN n.created",
                     "events",
                     {{"2024-03-14T09:30:00Z"}, {"2024-03-14T09:30:00Z"}});

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "events",
                     {{"created", "DateTime"}});
}

// A property the clause did not name keeps the type its text alone gives it, so a string
// that happens to read as a date is still the text it was written as
TEST_F(LoadJsonlDateTimeTest, LeavesAnUnnamedPropertyAsAString) {
    writeFixture("mixed.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z","sku":"2024-03-14"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "mixed.jsonl" AS mixed WITH DATETIMES ["created"])", _sessionGraph, loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "mixed",
                     {{"created", "DateTime"}, {"sku", "String"}});

    expectSortedRows("MATCH (n:Event) RETURN n.sku", "mixed", {{"2024-03-14"}});
}

TEST_F(LoadJsonlDateTimeTest, ReadsAnInstantOnAnEdge) {
    writeFixture("edges.jsonl",
                 R"({"type":"node","id":"0","labels":["Person"],"properties":{}}
{"type":"node","id":"1","labels":["Person"],"properties":{}}
{"type":"relationship","id":"0","label":"MET","properties":{"at":"2024-03-14T09:30:00Z"},"start":0,"end":1}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "edges.jsonl" AS edges WITH DATETIMES ["at"])", _sessionGraph, loadSink);

    expectSortedRows("MATCH ()-[e:MET]->() RETURN e.at", "edges", {{"2024-03-14T09:30:00Z"}});
}

TEST_F(LoadJsonlDateTimeTest, OrdersAndComparesTheInstantsItRead) {
    writeFixture("ordered.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"name":"b","created":"2024-03-14T09:30:00Z"}}
{"type":"node","id":"1","labels":["Event"],"properties":{"name":"a","created":"2019-01-01T00:00:00Z"}}
{"type":"node","id":"2","labels":["Event"],"properties":{"name":"c","created":"2026-09-23T14:05:00Z"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "ordered.jsonl" AS ordered WITH DATETIMES ["created"])", _sessionGraph, loadSink);

    StringRowSink sink;
    runQuery("MATCH (n:Event) RETURN n.name ORDER BY n.created ASC", "ordered", sink);
    EXPECT_EQ(sink.getRows(),
              (std::vector<StringRowSink::Row> {{"a"}, {"b"}, {"c"}}));

    expectSortedRows(R"(MATCH (n:Event) WHERE n.created > datetime("2024-01-01") RETURN n.name)",
                     "ordered",
                     {{"b"}, {"c"}});
}

// The query said the property holds an instant, so text naming none is malformed input
// rather than a property quietly written as something else
TEST_F(LoadJsonlDateTimeTest, RejectsAValueNamingNoInstant) {
    writeFixture("bad.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"not a date"}}
)");

    runQueryExpectingError(R"(LOAD JSONL "bad.jsonl" AS bad WITH DATETIMES ["created"])",
                           "failed to import");
}

TEST_F(LoadJsonlDateTimeTest, RejectsANamedPropertyThatIsNotAString) {
    writeFixture("number.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":1710408600}}
)");

    runQueryExpectingError(R"(LOAD JSONL "number.jsonl" AS number WITH DATETIMES ["created"])",
                           "failed to import");
}

// Both hint clauses are options of one statement, so a query may carry either, both, or
// neither, in whichever order it writes them
TEST_F(LoadJsonlDateTimeTest, ReadsBothHintClausesTogether) {
    writeFixture("both.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z","vec":[1.0,2.0,3.0]}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "both.jsonl" AS both WITH EMBEDDINGS [{"vec", 3}] WITH DATETIMES ["created"])",
             _sessionGraph,
             loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "both",
                     {{"created", "DateTime"}, {"vec", "Embedding"}});
}

TEST_F(LoadJsonlDateTimeTest, ReadsTheHintClausesInEitherOrder) {
    writeFixture("reordered.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z","vec":[1.0,2.0,3.0]}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "reordered.jsonl" AS reordered WITH DATETIMES ["created"] WITH EMBEDDINGS [{"vec", 3}])",
             _sessionGraph,
             loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "reordered",
                     {{"created", "DateTime"}, {"vec", "Embedding"}});
}

// An import naming no datetime property reads every string as text, as it did before the
// clause existed
TEST_F(LoadJsonlDateTimeTest, LeavesEveryStringAloneWithNoClause) {
    writeFixture("plain.jsonl",
                 R"({"type":"node","id":"0","labels":["Event"],"properties":{"created":"2024-03-14T09:30:00Z"}}
)");

    StringRowSink loadSink;
    runQuery(R"(LOAD JSONL "plain.jsonl" AS plain)", _sessionGraph, loadSink);

    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "plain",
                     {{"created", "String"}});
}
