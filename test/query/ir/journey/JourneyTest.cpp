#include "JourneyTest.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <initializer_list>
#include <optional>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

struct Station {
    NodeID node;
    std::string_view nlc;
};

struct Call {
    const Station* station {nullptr};
    std::optional<int64_t> arrive;
    std::optional<int64_t> depart;
};

Station addStation(GraphWriter& writer, std::string_view crs, std::string_view nlc, int64_t minInterchange, int64_t maxInterchange) {
    const NodeID station = writer.addNode({"STATION"});
    writer.addNodeProperty<types::String>(station, "crs", std::string_view {crs});
    writer.addNodeProperty<types::Int64>(station, "minInterchange", int64_t {minInterchange});
    writer.addNodeProperty<types::Int64>(station, "maxInterchange", int64_t {maxInterchange});

    return Station {station, nlc};
}

NodeID addCallingPoint(GraphWriter& writer, std::initializer_list<std::string_view> labels, std::string_view service, const Station& station) {
    const NodeID callingPoint = writer.addNode(labels);
    writer.addNodeProperty<types::String>(callingPoint, "service", std::string_view {service});
    writer.addNodeProperty<types::String>(callingPoint, "nlc", std::string_view {station.nlc});
    writer.addEdge("ARRIVE", callingPoint, station.node);

    return callingPoint;
}

// A calling point with a departure can be boarded, so its station has a day edge to it
void addService(GraphWriter& writer, std::string_view day, std::string_view service, std::initializer_list<Call> calls) {
    std::optional<NodeID> previous;
    for (const Call& call : calls) {
        const NodeID callingPoint = addCallingPoint(writer, {"MODE"}, service, *call.station);

        if (call.arrive) {
            writer.addNodeProperty<types::Int64>(callingPoint, "arrive", int64_t {*call.arrive});
        }

        if (call.depart) {
            writer.addNodeProperty<types::Int64>(callingPoint, "depart", int64_t {*call.depart});
            writer.addEdge(day, call.station->node, callingPoint);
        }

        if (previous) {
            writer.addEdge("NEXT", *previous, callingPoint);
        }

        previous = callingPoint;
    }
}

void addFixedLink(GraphWriter& writer, std::string_view service, const Station& from, const Station& to, int64_t start, int64_t end) {
    const NodeID opening = addCallingPoint(writer, {"MODE", "FIXED"}, service, from);
    writer.addNodeProperty<types::Int64>(opening, "start", int64_t {start});
    writer.addNodeProperty<types::Int64>(opening, "end", int64_t {end});
    writer.addEdge("WED", from.node, opening);

    const NodeID closing = addCallingPoint(writer, {"MODE"}, service, to);
    writer.addEdge("NEXT", opening, closing);
}

}

JourneyTest::JourneyTest() {
}

JourneyTest::~JourneyTest() {
}

void JourneyTest::initialize() {
    _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

    SystemAccessor system = _env->getSystemManager().accessUnique();
    buildTimetable(system.createGraph(_graphName));
}

void JourneyTest::buildTimetable(Graph* graph) {
    JobSystem jobSystem;
    jobSystem.init();

    GraphWriter writer(graph, &jobSystem);

    const Station oxford = addStation(writer, "OXF", "3115", 5, 60);
    const Station didcot = addStation(writer, "DID", "3128", 4, 45);
    const Station reading = addStation(writer, "RDG", "3149", 6, 50);
    const Station paddington = addStation(writer, "PAD", "3087", 10, 90);
    const Station swindon = addStation(writer, "SWI", "3138", 5, 40);

    addService(writer, "WED", "S1", {{&oxford, {}, 480}, {&didcot, 495, 497}, {&reading, 515, 517}, {&paddington, 545, {}}});
    addService(writer, "WED", "S2", {{&oxford, {}, 490}, {&didcot, 505, {}}});
    addService(writer, "WED", "S3", {{&oxford, {}, 500}, {&reading, 525, {}}});
    addService(writer, "WED", "S4", {{&oxford, {}, 485}, {&swindon, 520, {}}});
    addService(writer, "WED", "S5", {{&oxford, {}, 470}, {&didcot, 500, {}}});
    addService(writer, "WED", "S6", {{&oxford, {}, 475}, {&didcot, 510, {}}});
    addService(writer, "WED", "S7", {{&oxford, {}, 490}, {&didcot, 502, {}}});
    addService(writer, "WED", "S8", {{&oxford, {}, 520}, {&paddington, 580, {}}});
    addService(writer, "THU", "S9", {{&oxford, {}, 480}, {&paddington, 540, {}}});
    addService(writer, "WED", "S10", {{&oxford, {}, 482}, {&didcot, 497, 499}, {&swindon, 530, {}}});
    addService(writer, "WED", "S11", {{&didcot, {}, 510}, {&reading, 524, 526}, {&paddington, 556, {}}});
    addService(writer, "WED", "S12", {{&didcot, {}, 540}, {&paddington, 570, {}}});
    addService(writer, "WED", "S13", {{&didcot, {}, 505}, {&reading, 520, {}}});
    addService(writer, "WED", "S14", {{&reading, {}, 535}, {&paddington, 560, {}}});

    addFixedLink(writer, "F1", oxford, didcot, 420, 1380);
    addFixedLink(writer, "F2", oxford, reading, 600, 700);

    writer.submit();
    jobSystem.terminate();
}

void JourneyTest::findLegsFromOxford(std::string& query, std::string_view interchanges, int64_t departFrom, int64_t departBefore) {
    query += "MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) WITH o, d, ";
    query += interchanges;
    query += " AS interchanges, ";
    query += std::to_string(departFrom);
    query += " AS depart_from, ";
    query += std::to_string(departBefore);
    query += " AS depart_before ";
    query += FIND_LEGS;
}

void JourneyTest::keepUnbeatenLegsFromOxford(std::string& query) {
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH ";
    query += LEG_ROW;
    query += LEGS_AS_A_SORTED_LIST;
    query += "WITH ";
    query += KEEP_UNBEATEN_LEGS;
    query += " AS OTTlist ";
}

void JourneyTest::setChunkSize(size_t chunkSize) {
    _interpreter->setChunkSize(chunkSize);
}

void JourneyTest::run(std::string_view query, const ParameterMap* parameters, Rows& rows) {
    RowSink sink;
    QueryStatus status;
    _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink, parameters);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    rows = sink.rows();
}

void JourneyTest::expectRows(std::string_view query, const Rows& expected) {
    expectSortedRows(query, nullptr, expected);
}

void JourneyTest::expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
    expectSortedRows(query, &parameters, expected);
}

void JourneyTest::expectSortedRows(std::string_view query, const ParameterMap* parameters, const Rows& expected) {
    Rows actual;
    run(query, parameters, actual);
    std::sort(actual.begin(), actual.end());

    Rows sortedExpected = expected;
    std::sort(sortedExpected.begin(), sortedExpected.end());

    std::string actualText;
    describeRows(actual, actualText);

    EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
}

void JourneyTest::expectOrderedRows(std::string_view query, const Rows& expected) {
    Rows actual;
    run(query, nullptr, actual);

    std::string actualText;
    describeRows(actual, actualText);

    EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
}
