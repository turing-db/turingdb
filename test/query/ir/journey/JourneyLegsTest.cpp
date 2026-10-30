#include <gtest/gtest.h>

#include <stddef.h>

#include <string>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "JourneyTest.h"

using namespace db;
using namespace turing::test;

// The first query of the customer's planner: the legs that board at the origin inside the
// search window and alight at an interchange, with the window a connection there leaves in
class JourneyLegsTest : public JourneyTest {
};

TEST_F(JourneyLegsTest, findsTheLegsOfARound) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN ";
    query += LEG_ROW;
    query += ", length(leg1)";

    expectRows(query, {
        {"S1", "PAD", "[]", "3087", "480", "545", "555", "635", "true", "5"},
        {"S2", "DID", "[RDG, PAD]", "3128", "490", "505", "509", "550", "false", "3"},
        {"S3", "RDG", "[PAD]", "3149", "500", "525", "531", "575", "false", "3"},
        {"S5", "DID", "[RDG, PAD]", "3128", "470", "500", "504", "545", "false", "3"},
        {"S6", "DID", "[RDG, PAD]", "3128", "475", "510", "514", "555", "false", "3"},
        {"S7", "DID", "[RDG, PAD]", "3128", "490", "502", "506", "547", "false", "3"},
        {"S10", "DID", "[RDG, PAD]", "3128", "482", "497", "501", "542", "false", "3"},
        {"F1", "DID", "[RDG, PAD]", "3128", "null", "470", "474", "545", "false", "3"},
    });
}

TEST_F(JourneyLegsTest, ordersTheLegsByDepartureWithTheFixedLinkLast) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN firstStop.depart AS departure ORDER BY departure";

    expectOrderedRows(query, {{"470"}, {"475"}, {"480"}, {"482"}, {"490"}, {"490"}, {"500"}, {"null"}});
}

TEST_F(JourneyLegsTest, boardsAtBothEndsOfTheWindow) {
    std::string inclusive;
    findLegsFromOxford(inclusive, "['DID', 'RDG', 'PAD', 'SWI']", 470, 500);
    inclusive += "RETURN DISTINCT firstStop.service";

    expectRows(inclusive, {{"S1"}, {"S2"}, {"S3"}, {"S4"}, {"S5"}, {"S6"}, {"S7"}, {"S10"}, {"F1"}});

    std::string narrower;
    findLegsFromOxford(narrower, "['DID', 'RDG', 'PAD', 'SWI']", 471, 499);
    narrower += "RETURN DISTINCT firstStop.service";

    expectRows(narrower, {{"S1"}, {"S2"}, {"S4"}, {"S6"}, {"S7"}, {"S10"}, {"F1"}});
}

TEST_F(JourneyLegsTest, opensAFixedLinkOnlyWhileItRuns) {
    std::string bothLinks;
    findLegsFromOxford(bothLinks, "['DID', 'RDG', 'PAD']", 600, 650);
    bothLinks += "RETURN firstStop.service, inter.crs";

    expectRows(bothLinks, {{"F1", "DID"}, {"F2", "RDG"}});

    std::string afterTheSecondCloses;
    findLegsFromOxford(afterTheSecondCloses, "['DID', 'RDG', 'PAD']", 700, 760);
    afterTheSecondCloses += "RETURN firstStop.service, inter.crs";

    expectRows(afterTheSecondCloses, {{"F1", "DID"}});

    std::string beforeTheFirstOpens;
    findLegsFromOxford(beforeTheFirstOpens, "['DID', 'RDG', 'PAD']", 380, 420);
    beforeTheFirstOpens += "RETURN firstStop.service, inter.crs";

    expectRows(beforeTheFirstOpens, {});
}

// S1 runs on from Didcot and Reading to Paddington, so it is only a leg to Paddington. S10
// leaves the route at Didcot for Swindon, so alighting there is a leg.
TEST_F(JourneyLegsTest, alightsOnlyWhereTheServiceDoesNotRunOnToTheDestination) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD', 'SWI']", 470, 500);
    query += "AND firstStop.service IN ['S1', 'S10'] ";
    query += "RETURN firstStop.service, inter.crs, inter = d";

    expectRows(query, {{"S1", "PAD", "true"}, {"S10", "DID", "false"}, {"S10", "SWI", "false"}});
}

// Of the 36 calling points, 9 run on to Paddington in two hops or more: the first three of
// S1, the first of S8 and S9, the first two of S11, and the first of S12 and S14
TEST_F(JourneyLegsTest, spellsTheRunOnAsAQuantifiedOrAVariableLengthRelationship) {
    expectRows("MATCH (d:STATION {crs: 'PAD'}) MATCH (l:MODE) "
               "WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE]->{2,20}(d)} RETURN count(l)",
               {{"27"}});

    expectRows("MATCH (d:STATION {crs: 'PAD'}) MATCH (l:MODE) "
               "WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE*2..20]->(d)} RETURN count(l)",
               {{"27"}});
}

// The first calling point ARRIVEs at the origin itself, so a zero-hop leg ends there. S1's
// is dropped: its service runs on to Paddington.
TEST_F(JourneyLegsTest, takesALegThatNeverLeavesItsFirstStop) {
    std::string query;
    findLegsFromOxford(query, "['OXF']", 470, 500);
    query += "RETURN firstStop.service, length(leg1), firstStop = lastStop";

    expectRows(query, {
        {"S2", "2", "true"},
        {"S3", "2", "true"},
        {"S4", "2", "true"},
        {"S5", "2", "true"},
        {"S6", "2", "true"},
        {"S7", "2", "true"},
        {"S10", "2", "true"},
        {"F1", "2", "true"},
    });
}

TEST_F(JourneyLegsTest, carriesTheLegsIntoTheNextQuery) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN o, d, inter AS prevCalling, leg1 AS path, firstStop.depart AS departure ORDER BY departure ";
    query += "NEXT ";
    query += "RETURN o.crs, d.crs, prevCalling.crs, [n IN nodes(path) | coalesce(n.crs, n.service)], departure";

    expectRows(query, {
        {"OXF", "PAD", "DID", "[OXF, S5, S5, DID]", "470"},
        {"OXF", "PAD", "DID", "[OXF, S6, S6, DID]", "475"},
        {"OXF", "PAD", "PAD", "[OXF, S1, S1, S1, S1, PAD]", "480"},
        {"OXF", "PAD", "DID", "[OXF, S10, S10, DID]", "482"},
        {"OXF", "PAD", "DID", "[OXF, S2, S2, DID]", "490"},
        {"OXF", "PAD", "DID", "[OXF, S7, S7, DID]", "490"},
        {"OXF", "PAD", "RDG", "[OXF, S3, S3, RDG]", "500"},
        {"OXF", "PAD", "DID", "[OXF, F1, F1, DID]", "null"},
    });
}

TEST_F(JourneyLegsTest, findsTheSameLegsAtEveryChunkSize) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN firstStop.service, inter.crs, coalesce(lastStop.arrive, depart_from)";

    for (const size_t chunkSize : {1, 2, 3, 5}) {
        SCOPED_TRACE(chunkSize);
        setChunkSize(chunkSize);

        expectRows(query, {
            {"S1", "PAD", "545"},
            {"S2", "DID", "505"},
            {"S3", "RDG", "525"},
            {"S5", "DID", "500"},
            {"S6", "DID", "510"},
            {"S7", "DID", "502"},
            {"S10", "DID", "497"},
            {"F1", "DID", "470"},
        });
    }
}

// The customer's search as written, its window given as parameters.
// Disabled: an expression parses and types a parameter, but codegen cannot read one yet (TUR-48).
TEST_F(JourneyLegsTest, DISABLED_readsTheWindowFromParameters) {
    ParameterMap parameters;

    ParameterValue departFrom;
    departFrom.setInt64(470);
    parameters.set("depart_from", departFrom);

    ParameterValue departBefore;
    departBefore.setInt64(500);
    parameters.set("depart_before", departBefore);

    expectRows("MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) "
               "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges "
               "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
               "WHERE inter.crs IN interchanges "
               "AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} "
               "AND CASE WHEN firstStop:FIXED "
               "THEN ( $depart_before > firstStop.start AND $depart_from < firstStop.end ) "
               "ELSE $depart_before >= firstStop.depart >= $depart_from "
               "END "
               "RETURN firstStop.service, "
               "coalesce(lastStop.arrive, $depart_from) + inter.minInterchange AS departAfter, "
               "coalesce(lastStop.arrive, $depart_before) + inter.maxInterchange AS departBefore",
               parameters,
               {
                   {"S1", "555", "635"},
                   {"S2", "509", "550"},
                   {"S3", "531", "575"},
                   {"S5", "504", "545"},
                   {"S6", "514", "555"},
                   {"S7", "506", "547"},
                   {"S10", "501", "542"},
                   {"F1", "474", "545"},
               });
}

// Disabled: coll.indexOf does not exist. The coll.* functions need the function namespaces
// TUR-298 asks for.
TEST_F(JourneyLegsTest, DISABLED_slicesTheInterchangesAtCollIndexOf) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "RETURN firstStop.service, interchanges[coll.indexOf(interchanges, inter.crs)+1..]";

    expectRows(query, {
        {"S1", "[]"},
        {"S2", "[RDG, PAD]"},
        {"S3", "[PAD]"},
        {"S5", "[RDG, PAD]"},
        {"S6", "[RDG, PAD]"},
        {"S7", "[RDG, PAD]"},
        {"S10", "[RDG, PAD]"},
        {"F1", "[RDG, PAD]"},
    });
}
