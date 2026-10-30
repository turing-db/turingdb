#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "JourneyTest.h"

using namespace db;
using namespace turing::test;

// The second query of the customer's planner: the legs of a round collected into OTTlist,
// sorted by location then arrival, and folded by a reduce that keeps, at each location, only
// the legs no other leg beats by leaving later and arriving earlier
class JourneyParetoTest : public JourneyTest {
protected:
    void expectKept(std::string_view legs, std::string_view keptServices) {
        std::string query = "WITH ";
        query += legs;
        query += " AS OTTlist RETURN [leg IN ";
        query += KEEP_UNBEATEN_LEGS;
        query += " | leg.service]";

        expectRows(query, {{std::string(keptServices)}});
    }
};

TEST_F(JourneyParetoTest, readsTheLastLegAndSlicesItOff) {
    expectRows("WITH [{nlc: 'A', departure: 480, arrival: 500}, {nlc: 'A', departure: 470, arrival: 510}] AS l "
               "RETURN l[-1].nlc, l[..-1], l + {nlc: 'B'}, [l[0]], size(l)",
               {{"A",
                 "[{arrival: 500, departure: 480, nlc: A}]",
                 "[{arrival: 500, departure: 480, nlc: A}, {arrival: 510, departure: 470, nlc: A}, {nlc: B}]",
                 "[{arrival: 500, departure: 480, nlc: A}]",
                 "2"}});
}

// A null sorts last ascending and first descending, so the fixed links lead
TEST_F(JourneyParetoTest, collectsTheServicesLatestFirst) {
    expectRows("MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) WITH f ORDER BY f.depart DESC, f.service "
               "RETURN collect(f.service)",
               {{"[F1, F2, S8, S3, S2, S7, S4, S10, S1, S6, S5]"}});
}

TEST_F(JourneyParetoTest, buildsTheLegsFromCollectedCallingPoints) {
    expectRows("MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) WITH f ORDER BY f.depart WITH collect(f) AS fs "
               "RETURN [x IN fs WHERE x.depart IS NOT NULL | {service: x.service, departure: x.depart}][0..2]",
               {{"[{departure: 470, service: S5}, {departure: 475, service: S6}]"}});
}

TEST_F(JourneyParetoTest, dropsALegThatLeavesNoLaterAndArrivesNoEarlier) {
    expectKept("[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'A', departure: 470, arrival: 500}, "
               "{service: 'c', nlc: 'A', departure: 482, arrival: 497}]",
               "[a]");
}

TEST_F(JourneyParetoTest, keepsALegThatLeavesLaterAndArrivesLater) {
    expectKept("[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'A', departure: 490, arrival: 502}]",
               "[a, b]");
}

TEST_F(JourneyParetoTest, replacesTheLastLegWithOneLeavingWithItButArrivingEarlier) {
    expectKept("[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'A', departure: 490, arrival: 502}, "
               "{service: 'c', nlc: 'A', departure: 490, arrival: 501}]",
               "[a, c]");
}

TEST_F(JourneyParetoTest, startsAgainAtEachLocation) {
    expectKept("[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'B', departure: 470, arrival: 500}]",
               "[a, b]");
}

// A fixed link has no departure, so both comparisons with it are unknown and neither WHEN
// holds: the leg after it is kept
TEST_F(JourneyParetoTest, keepsALegAfterAFixedLinkThatHasNoDeparture) {
    expectKept("[{service: 'f', nlc: 'A', departure: null, arrival: 470}, "
               "{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'A', departure: 470, arrival: 500}]",
               "[f, a]");
}

// OTTlist[0] of no leg is null, so the reduce starts from, and returns, a list holding a null
TEST_F(JourneyParetoTest, keepsANullWhenThereIsNoLeg) {
    expectKept("[]", "[null]");
}

TEST_F(JourneyParetoTest, sortsTheLegsByLocationThenArrival) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH ";
    query += LEG_ROW;
    query += LEGS_AS_A_SORTED_LIST;
    query += "RETURN [leg IN OTTlist | leg.service]";

    expectRows(query, {{"[S3, F1, S10, S5, S7, S2, S6, S1]"}});
}

// At Didcot S10 beats S5, and S7 beats S2 and S6
TEST_F(JourneyParetoTest, keepsTheLegsOfARoundNoOtherBeats) {
    std::string query;
    keepUnbeatenLegsFromOxford(query);
    query += "RETURN [leg IN OTTlist | leg.service]";

    expectRows(query, {{"[S3, F1, S10, S7, S1]"}});
}

// The customer's last sort, apoc.coll.sortMulti(OTTlist, ['^departure', 'arrived', '^arrival'])
TEST_F(JourneyParetoTest, ordersTheKeptLegsByDepartureThenArrival) {
    std::string query;
    keepUnbeatenLegsFromOxford(query);
    query += "UNWIND OTTlist AS leg ";
    query += "RETURN leg.service, leg.departure, leg.arrival, leg.arrived ";
    query += "ORDER BY leg.departure, leg.arrived DESC, leg.arrival";

    expectOrderedRows(query, {
        {"S1", "480", "545", "true"},
        {"S10", "482", "497", "false"},
        {"S7", "490", "502", "false"},
        {"S3", "500", "525", "false"},
        {"F1", "null", "470", "false"},
    });
}

TEST_F(JourneyParetoTest, keepsTheSameLegsAtEveryChunkSize) {
    std::string query;
    keepUnbeatenLegsFromOxford(query);
    query += "RETURN [leg IN OTTlist | leg.service]";

    for (const size_t chunkSize : {1, 2, 3, 5}) {
        SCOPED_TRACE(chunkSize);
        setChunkSize(chunkSize);

        expectRows(query, {{"[S3, F1, S10, S7, S1]"}});
    }
}

TEST_F(JourneyParetoTest, keepsTheLegsTheQueryBeforeReturned) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN ";
    query += LEG_ROW;
    query += "ORDER BY departure ";
    query += "NEXT ";
    query += LEGS_AS_A_SORTED_LIST;
    query += "WITH ";
    query += KEEP_UNBEATEN_LEGS;
    query += " AS OTTlist ";
    query += "RETURN [leg IN OTTlist | leg.service]";

    expectRows(query, {{"[S3, F1, S10, S7, S1]"}});
}

// The customer's collect of one map per leg.
// Disabled: collect() rejects a map argument in the analyzer (TUR-100).
TEST_F(JourneyParetoTest, DISABLED_collectsTheLegsAsMaps) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH ";
    query += LEG_ROW;
    query += "ORDER BY nlc DESC, arrival, departure DESC ";
    query += "WITH collect({service: service, nlc: nlc, departure: departure, arrival: arrival}) AS OTTlist ";
    query += "WITH ";
    query += KEEP_UNBEATEN_LEGS;
    query += " AS OTTlist ";
    query += "RETURN [leg IN OTTlist | leg.service]";

    expectRows(query, {{"[S3, F1, S10, S7, S1]"}});
}

// sortMulti sorts descending on a key, and ascending on one written with a ^.
// Disabled: apoc.coll.sortMulti does not exist (TUR-298).
TEST_F(JourneyParetoTest, DISABLED_sortsTheLegsWithSortMulti) {
    expectRows("WITH [{service: 'a', nlc: 'A', departure: 482, arrival: 497}, "
               "{service: 'b', nlc: 'B', departure: 470, arrival: 500}, "
               "{service: 'c', nlc: 'A', departure: 490, arrival: 502}, "
               "{service: 'd', nlc: 'A', departure: 470, arrival: 500}] AS OTTlist "
               "RETURN [leg IN apoc.coll.sortMulti(OTTlist, ['nlc', '^arrival', 'departure']) | leg.service]",
               {{"[b, a, d, c]"}});
}

// Disabled: a key of a node held in a map is read as a key of a map held by another map,
// which the analyzer rejects.
TEST_F(JourneyParetoTest, DISABLED_readsTheStationOfALeg) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH [{service: firstStop.service, prevCalling: inter}] AS legs ";
    query += "UNWIND legs AS leg ";
    query += "RETURN leg.service, leg.prevCalling.crs";

    expectRows(query, {
        {"S1", "PAD"},
        {"S2", "DID"},
        {"S3", "RDG"},
        {"S5", "DID"},
        {"S6", "DID"},
        {"S7", "DID"},
        {"S10", "DID"},
        {"F1", "DID"},
    });
}

TEST_F(JourneyParetoTest, keepsThePathOfALeg) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH {service: firstStop.service, path: leg1} AS leg ";
    query += "RETURN leg.service, length(leg.path)";

    expectRows(query, {
        {"S1", "5"},
        {"S2", "3"},
        {"S3", "3"},
        {"S5", "3"},
        {"S6", "3"},
        {"S7", "3"},
        {"S10", "3"},
        {"F1", "3"},
    });
}
