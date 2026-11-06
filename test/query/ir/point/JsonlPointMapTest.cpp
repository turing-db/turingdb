#include <gtest/gtest.h>

#include <string_view>
#include <utility>
#include <vector>

#include "PointImportTest.h"

using namespace turing::test;

// LOAD JSONL reads the object of a WITH POINTS property by the rules point() reads a map
// with: it ignores a key it does not know unless that key holds null, which makes point()
// null, and raises point()'s own error for a known key holding a value of the wrong kind.
class JsonlPointMapTest : public PointImportTest {
};

TEST_F(JsonlPointMapTest, IgnoresAKeyPointDoesNotKnowHoldingAnyValueButNull) {
    writeFixture("tagged.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"longitude":2.35,"latitude":48.85,"verified":true,"tags":["a",1],"source":{"name":"gps"}}}}
)");

    load(R"(LOAD JSONL "tagged.jsonl" AS tagged WITH POINTS [at])");

    expectSortedRows("MATCH (n:City) RETURN n.at", "tagged", {{"point({x: 2.35, y: 48.85, crs: 'wgs-84'})"}});

    expectSortedRows("RETURN point({longitude: 2.35, latitude: 48.85, verified: true, tags: ['a', 1], source: {name: 'gps'}})",
                     _sessionGraph,
                     {{"point({x: 2.35, y: 48.85, crs: 'wgs-84'})"}});
}

TEST_F(JsonlPointMapTest, RejectsAKnownKeyHoldingAValueOfTheWrongKindAsPointDoes) {
    const std::vector<std::pair<std::string_view, std::string_view>> cases = {
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":true,"y":2}}})",
         "property 'at': point() reads a number for 'x', not 'Bool'"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":[1],"y":2}}})",
         "property 'at': point() reads a number for 'x', not 'List'"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":{"value":1},"y":2}}})",
         "property 'at': point() reads a number for 'x', not 'Map'"},
        {R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":1,"y":2,"crs":false}}})",
         "property 'at': point() reads a string for 'crs', not 'Bool'"},
    };

    for (const auto& [line, reason] : cases) {
        writeFixture("bad.jsonl", line);
        runQueryExpectingError(R"(LOAD JSONL "bad.jsonl" AS bad WITH POINTS [at])", reason);
    }

    runQueryExpectingError("RETURN point({x: true, y: 2})", "point() reads a number for 'x', not 'Bool'");
    runQueryExpectingError("RETURN point({x: [1], y: 2})", "point() reads a number for 'x', not 'List'");
    runQueryExpectingError("RETURN point({x: {value: 1}, y: 2})", "point() reads a number for 'x', not 'Map'");
    runQueryExpectingError("RETURN point({x: 1, y: 2, crs: false})", "point() reads a string for 'crs', not 'Bool'");
}

TEST_F(JsonlPointMapTest, RejectsANullUnderAKeyPointDoesNotKnowAsPointIsNull) {
    writeFixture("unset.jsonl",
                 R"({"type":"node","id":"0","labels":["City"],"properties":{"at":{"x":1,"y":2,"accuracy":null}}}
)");

    runQueryExpectingError(R"(LOAD JSONL "unset.jsonl" AS unset WITH POINTS [at])", "property 'at' holds a null");

    expectSortedRows("RETURN point({x: 1, y: 2, accuracy: null})", _sessionGraph, {{"null"}});
}
