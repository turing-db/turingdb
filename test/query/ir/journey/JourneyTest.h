#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "TuringTest.h"

namespace db {
class Graph;
class ParameterMap;
class QueryInterpreterV3;
}

namespace turing::test {

class TuringTestEnv;

// A rail timetable after a customer's ticketing graph. A service is a chain of MODE calling
// points joined by NEXT, each ARRIVEs at its STATION, and a WED edge runs from a station to
// each calling point boardable there. A FIXED calling point opens a link that runs between a
// start and an end time. Times are minutes after midnight.
class JourneyTest : public TuringTest {
public:
    JourneyTest();
    ~JourneyTest() override;

    void initialize() override;

protected:
    // The customer's leg search, with its two parameters read from variables: a leg boards
    // at o between depart_from and depart_before and alights at one of interchanges, where
    // its service does not run on to the destination d
    static constexpr std::string_view FIND_LEGS =
        "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) "
        "WHERE inter.crs IN interchanges "
        "AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} "
        "AND CASE WHEN firstStop:FIXED "
        "THEN (depart_before > firstStop.start AND depart_from < firstStop.end) "
        "ELSE depart_before >= firstStop.depart >= depart_from "
        "END ";

    // The columns the customer returns for a leg, with its service and stop named. The
    // interchanges left are sliced past a reduce, which stands in for coll.indexOf
    static constexpr std::string_view LEG_ROW =
        "firstStop.service AS service, "
        "inter.crs AS stop, "
        "interchanges[reduce(found = -1, i IN range(0, size(interchanges) - 1) | "
        "CASE WHEN found = -1 AND interchanges[i] = inter.crs THEN i ELSE found END) + 1..] AS interchanges, "
        "lastStop.nlc AS nlc, "
        "firstStop.depart AS departure, "
        "coalesce(lastStop.arrive, depart_from) AS arrival, "
        "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, "
        "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore, "
        "inter = d AS arrived ";

    // The rows of LEG_ROW as OTTlist, a list of one map per leg sorted the way the
    // customer's apoc.coll.sortMulti(OTTlist, ['nlc', '^arrival', 'departure']) sorts it.
    // Each row is collected as one list: collect() takes no map yet, and drops nulls.
    static constexpr std::string_view LEGS_AS_A_SORTED_LIST =
        "WITH service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived "
        "ORDER BY nlc DESC, arrival, departure DESC "
        "WITH collect([service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived]) AS rows "
        "WITH [r IN rows | {service: r[0], stop: r[1], interchanges: r[2], nlc: r[3], departure: r[4], "
        "arrival: r[5], departAfter: r[6], departBefore: r[7], arrived: r[8]}] AS OTTlist ";

    // The customer's reduce over OTTlist, verbatim: at each location (nlc), a leg is dropped
    // when the leg kept before it leaves no earlier and arrives no later
    static constexpr std::string_view KEEP_UNBEATEN_LEGS =
        "reduce(retain = [OTTlist[0]], i IN range(1, size(OTTlist) - 1) | "
        "CASE "
        "WHEN OTTlist[i].nlc = retain[-1].nlc THEN "
        "CASE "
        "WHEN (OTTlist[i].departure <= retain[-1].departure AND OTTlist[i].arrival >= retain[-1].arrival) THEN retain "
        "WHEN (OTTlist[i].departure = retain[-1].departure AND OTTlist[i].arrival < retain[-1].arrival) THEN retain[..-1] + OTTlist[i] "
        "ELSE retain + OTTlist[i] "
        "END "
        "ELSE retain + OTTlist[i] "
        "END)";

    // Writes the search for the legs from Oxford toward Paddington that board between
    // @param departFrom and @param departBefore, up to FIND_LEGS
    void findLegsFromOxford(std::string& query, std::string_view interchanges, int64_t departFrom, int64_t departBefore);

    // Writes the search, the legs as OTTlist and the reduce, leaving the kept legs as OTTlist
    void keepUnbeatenLegsFromOxford(std::string& query);

    void setChunkSize(size_t chunkSize);

    void expectRows(std::string_view query, const Rows& expected);
    void expectRows(std::string_view query, const db::ParameterMap& parameters, const Rows& expected);
    void expectOrderedRows(std::string_view query, const Rows& expected);

private:
    const std::string _graphName {"timetable"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::QueryInterpreterV3> _interpreter;

    void buildTimetable(db::Graph* graph);
    void run(std::string_view query, const db::ParameterMap* parameters, Rows& rows);
    void expectSortedRows(std::string_view query, const db::ParameterMap* parameters, const Rows& expected);
};

}
