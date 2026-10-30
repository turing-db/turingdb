#include <gtest/gtest.h>

#include <stddef.h>

#include <string>

#include "JourneyTest.h"

using namespace db;
using namespace turing::test;

// The legs FIND_LEGS matches from Oxford between 470 and 500 are variable-length paths:
// S1 runs 5 hops to Paddington, S3 3 hops to Reading, and S2, S5, S6, S7, S10 and F1 3 hops
// to Didcot. They arrive at 545, 525, 505, 500, 510, 502 and 497, and F1, a fixed link, at
// no set time. Collected into one list, the legs are folded over as paths.
class JourneyLegPathsTest : public JourneyTest {
protected:
    void collectLegsFromOxford(std::string& query) {
        findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
        query += "WITH collect(leg1) AS legs ";
    }

    void expectFoldsOverTheLegs() {
        std::string query;
        collectLegsFromOxford(query);
        query += "RETURN size(legs), "
                 "reduce(longest = 0, l IN legs | CASE WHEN length(l) > longest THEN length(l) ELSE longest END), "
                 "reduce(hops = 0, l IN legs | hops + length(l)), "
                 "reduce(callingPoints = 0, l IN legs | callingPoints + size([n IN nodes(l) WHERE n:MODE]))";

        expectRows(query, {{"8", "5", "26", "18"}});
    }

    void expectKeepsTheEarliestArrival() {
        std::string query;
        collectLegsFromOxford(query);
        query += "WITH reduce(best = null, l IN legs | "
                 "CASE WHEN nodes(l)[-2].arrive IS NULL THEN best "
                 "WHEN best IS NULL OR nodes(l)[-2].arrive < nodes(best)[-2].arrive THEN l "
                 "ELSE best END) AS best "
                 "RETURN nodes(best)[1].service, nodes(best)[-2].arrive, nodes(best)[-1].crs, length(best)";

        expectRows(query, {{"S10", "497", "DID", "3"}});
    }

    // F1 has no arrival, so its comparison is null and the CASE drops it
    void expectKeepsTheLegsArrivingBefore505() {
        std::string query;
        collectLegsFromOxford(query);
        query += "WITH reduce(kept = [], l IN legs | CASE WHEN nodes(l)[-2].arrive < 505 THEN kept + l ELSE kept END) AS kept "
                 "UNWIND kept AS l "
                 "RETURN nodes(l)[1].service, nodes(l)[-2].arrive, length(l)";

        expectRows(query, {
            {"S5", "500", "3"},
            {"S7", "502", "3"},
            {"S10", "497", "3"},
        });
    }

    // In order of departure, a leg is kept only when it arrives before the leg kept last:
    // S5 at 500, then S10 at 497
    void expectKeepsTheLegsThatArriveEarlierThanTheLastKept() {
        std::string query;
        findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
        query += "WITH leg1, firstStop.depart AS departure, firstStop.service AS service "
                 "ORDER BY departure, service "
                 "WITH collect(leg1) AS legs "
                 "WITH reduce(kept = [], l IN legs | "
                 "CASE WHEN size(kept) = 0 OR nodes(l)[-2].arrive < nodes(kept[-1])[-2].arrive THEN kept + l "
                 "ELSE kept END) AS kept "
                 "RETURN [l IN kept | nodes(l)[1].service]";

        expectRows(query, {{"[S5, S10]"}});
    }
};

TEST_F(JourneyLegPathsTest, foldsOverTheCollectedLegs) {
    expectFoldsOverTheLegs();
}

TEST_F(JourneyLegPathsTest, keepsTheEarliestArrivingLegAsTheAccumulator) {
    expectKeepsTheEarliestArrival();
}

TEST_F(JourneyLegPathsTest, keepsTheLegsArrivingBefore505) {
    expectKeepsTheLegsArrivingBefore505();
}

TEST_F(JourneyLegPathsTest, keepsTheLegsThatArriveEarlierThanTheLastKept) {
    expectKeepsTheLegsThatArriveEarlierThanTheLastKept();
}

TEST_F(JourneyLegPathsTest, foldsOverTheLegsOfEachInterchange) {
    std::string query;
    findLegsFromOxford(query, "['DID', 'RDG', 'PAD']", 470, 500);
    query += "WITH inter.crs AS stop, collect(leg1) AS legs "
             "RETURN stop, size(legs), reduce(hops = 0, l IN legs | hops + length(l)), "
             "reduce(interchange = 0, l IN legs | interchange + nodes(l)[-1].minInterchange)";

    expectRows(query, {
        {"DID", "6", "18", "24"},
        {"RDG", "1", "3", "6"},
        {"PAD", "1", "5", "10"},
    });
}

// Two rows per step, so the legs reach the collect over several steps, each building its
// paths over the last one's
TEST_F(JourneyLegPathsTest, foldsOverLegsCollectedAcrossSteps) {
    setChunkSize(2);

    expectFoldsOverTheLegs();
    expectKeepsTheEarliestArrival();
    expectKeepsTheLegsArrivingBefore505();
    expectKeepsTheLegsThatArriveEarlierThanTheLastKept();
}
