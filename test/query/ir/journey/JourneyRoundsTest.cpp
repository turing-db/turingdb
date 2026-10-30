#include <gtest/gtest.h>

#include <string>

#include "JourneyTest.h"

using namespace db;
using namespace turing::test;

// The round after the pruning, as the columns the customer returns for a leg suggest: each
// kept leg not yet at the destination searches on from its stop, inside the window a
// connection there leaves in and toward the interchanges it has left
class JourneyRoundsTest : public JourneyTest {
protected:
    void searchOnFromTheKeptLegs(std::string& query) {
        query += "UNWIND OTTlist AS leg WITH leg WHERE NOT leg.arrived ";
        query += "MATCH (o:STATION {crs: leg.stop}), (d:STATION {crs: 'PAD'}) ";
        query += "WITH leg, o, d, leg.interchanges AS interchanges, ";
        query += "leg.departAfter AS depart_from, leg.departBefore AS depart_before ";
        query += FIND_LEGS;
    }
};

// F1 reaches Didcot at any time, so it catches S1 there as well as the trains S10 and S7
// connect into. S3 reaches Reading after S1 and S11 have left it.
TEST_F(JourneyRoundsTest, searchesOnFromEveryKeptLegNotYetArrived) {
    std::string query;
    keepUnbeatenLegsFromOxford(query);
    searchOnFromTheKeptLegs(query);
    query += "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive, inter = d";

    expectRows(query, {
        {"F1", "S1", "PAD", "545", "true"},
        {"F1", "S11", "PAD", "556", "true"},
        {"F1", "S12", "PAD", "570", "true"},
        {"F1", "S13", "RDG", "520", "false"},
        {"S10", "S11", "PAD", "556", "true"},
        {"S10", "S12", "PAD", "570", "true"},
        {"S10", "S13", "RDG", "520", "false"},
        {"S3", "S14", "PAD", "560", "true"},
        {"S7", "S11", "PAD", "556", "true"},
        {"S7", "S12", "PAD", "570", "true"},
    });
}

TEST_F(JourneyRoundsTest, chainsTheRoundsWithNext) {
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
    query += "RETURN OTTlist ";
    query += "NEXT ";
    searchOnFromTheKeptLegs(query);
    query += "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive";

    expectRows(query, {
        {"F1", "S1", "PAD", "545"},
        {"F1", "S11", "PAD", "556"},
        {"F1", "S12", "PAD", "570"},
        {"F1", "S13", "RDG", "520"},
        {"S10", "S11", "PAD", "556"},
        {"S10", "S12", "PAD", "570"},
        {"S10", "S13", "RDG", "520"},
        {"S3", "S14", "PAD", "560"},
        {"S7", "S11", "PAD", "556"},
        {"S7", "S12", "PAD", "570"},
    });
}

TEST_F(JourneyRoundsTest, ranksTheJourneysThatReachTheDestination) {
    std::string query;
    keepUnbeatenLegsFromOxford(query);
    searchOnFromTheKeptLegs(query);
    query += "AND inter = d ";
    query += "RETURN leg.service AS first, firstStop.service AS second, lastStop.arrive AS arrival ";
    query += "ORDER BY arrival, first, second";

    expectOrderedRows(query, {
        {"F1", "S1", "545"},
        {"F1", "S11", "556"},
        {"S10", "S11", "556"},
        {"S7", "S11", "556"},
        {"S3", "S14", "560"},
        {"F1", "S12", "570"},
        {"S10", "S12", "570"},
        {"S7", "S12", "570"},
    });
}

// The customer's leg map holds the destination and the path, so the next round boards at
// the path's last node and searches toward the destination it carries.
// Disabled: collect() takes no map (TUR-100), a path held in a map fails to execute
// (TUR-153), and a node read out of a map cannot start a MATCH.
TEST_F(JourneyRoundsTest, DISABLED_boardsTheNextRoundAtTheEndOfTheLegsPath) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH ";
    query += LEG_ROW;
    query += ", d AS destination, leg1 AS path ";
    query += "ORDER BY nlc DESC, arrival, departure DESC ";
    query += "WITH collect({service: service, interchanges: interchanges, nlc: nlc, departure: departure, "
             "arrival: arrival, departAfter: departAfter, departBefore: departBefore, arrived: arrived, "
             "destination: destination, path: path}) AS OTTlist ";
    query += "WITH ";
    query += KEEP_UNBEATEN_LEGS;
    query += " AS OTTlist ";
    query += "UNWIND OTTlist AS leg WITH leg WHERE NOT leg.arrived ";
    query += "WITH leg, last(nodes(leg.path)) AS o, leg.destination AS d, leg.interchanges AS interchanges, ";
    query += "leg.departAfter AS depart_from, leg.departBefore AS depart_before ";
    query += FIND_LEGS;
    query += "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive";

    expectRows(query, {
        {"F1", "S1", "PAD", "545"},
        {"F1", "S11", "PAD", "556"},
        {"F1", "S12", "PAD", "570"},
        {"F1", "S13", "RDG", "520"},
        {"S10", "S11", "PAD", "556"},
        {"S10", "S12", "PAD", "570"},
        {"S10", "S13", "RDG", "520"},
        {"S3", "S14", "PAD", "560"},
        {"S7", "S11", "PAD", "556"},
        {"S7", "S12", "PAD", "570"},
    });
}
