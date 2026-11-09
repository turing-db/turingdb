#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A fold whose accumulator widens lowers its body twice, so each query runs beside its
// control, whose Double initial value lowers the body once. Out-degrees: Remy 4, Adam 3,
// Maxime, Luc, Suhas and Cyrus 2, Martina and Doruk 1. Only Remy and Adam start a 2-hop
// path, 4 each.
class LimitInWidenedFoldTest : public WriteQueryTest {
};

TEST_F(LimitInWidenedFoldTest, countsALimitedMatch) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "2.000000"},
                           {"Luc", "2.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "2.000000"},
                           {"Cyrus", "2.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (n)-->(m) RETURN m LIMIT 3 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (n)-->(m) RETURN m LIMIT 3 })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, collectsALimitedMatch) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "2.000000"},
                           {"Luc", "2.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "2.000000"},
                           {"Cyrus", "2.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * size(COLLECT { MATCH (n)-->(m) RETURN m.name LIMIT 3 }))",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * size(COLLECT { MATCH (n)-->(m) RETURN m.name LIMIT 3 }))",
               expected);
}

TEST_F(LimitInWidenedFoldTest, checksALimitedMatchExists) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | "
               "CASE WHEN EXISTS { MATCH (n)-->(m)-->(k) RETURN k LIMIT 1 } THEN acc + 0.5 ELSE acc END)",
               {{"Remy", "1.000000"},
                {"Adam", "1.000000"},
                {"Maxime", "0.000000"},
                {"Luc", "0.000000"},
                {"Martina", "0.000000"},
                {"Suhas", "0.000000"},
                {"Cyrus", "0.000000"},
                {"Doruk", "0.000000"}});
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | "
               "CASE WHEN EXISTS { MATCH (n)-->(m)-->(k) RETURN k LIMIT 1 } THEN acc + 0.5 ELSE acc END)",
               {{"Remy", "1.000000"},
                {"Adam", "1.000000"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(LimitInWidenedFoldTest, countsALimitedTwoHopMatch) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "0.000000"},
                           {"Luc", "0.000000"},
                           {"Martina", "0.000000"},
                           {"Suhas", "0.000000"},
                           {"Cyrus", "0.000000"},
                           {"Doruk", "0.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (n)-->(m)-->(k) RETURN k LIMIT 3 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (n)-->(m)-->(k) RETURN k LIMIT 3 })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, countsALimitedLabelScan) {
    expectRows("RETURN reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (m:Interest) RETURN m LIMIT 3 })",
               {{"3.000000"}});
    expectRows("RETURN reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { MATCH (m:Interest) RETURN m LIMIT 3 })",
               {{"3.000000"}});
}

TEST_F(LimitInWidenedFoldTest, countsALimitedMatchInAnAllReduce) {
    const Rows expected = {{"Remy", "false"},
                           {"Adam", "false"},
                           {"Maxime", "true"},
                           {"Luc", "true"},
                           {"Martina", "true"},
                           {"Suhas", "true"},
                           {"Cyrus", "true"},
                           {"Doruk", "true"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, allReduce(s = 0.0, x IN [1, 2] | s + 0.5 * COUNT { MATCH (n)-->(m) RETURN m LIMIT 3 }, s < 2.5)",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, allReduce(s = 0, x IN [1, 2] | s + 0.5 * COUNT { MATCH (n)-->(m) RETURN m LIMIT 3 }, s < 2.5)",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsACallRunPerRow) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "2.000000"},
                           {"Luc", "2.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "2.000000"},
                           {"Cyrus", "2.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) WITH count(m) AS c "
               "CALL (c) { UNWIND range(1, c) AS i RETURN i LIMIT 10 } "
               "RETURN i LIMIT 3 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) WITH count(m) AS c "
               "CALL (c) { UNWIND range(1, c) AS i RETURN i LIMIT 10 } "
               "RETURN i LIMIT 3 })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsAnOptionalCallRunPerRow) {
    const Rows expected = {{"Remy", "2.000000"},
                           {"Adam", "2.000000"},
                           {"Maxime", "1.000000"},
                           {"Luc", "1.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "1.000000"},
                           {"Cyrus", "1.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) WITH count(m) AS c "
               "OPTIONAL CALL (c) { UNWIND range(2, c) AS i RETURN i LIMIT 10 } "
               "RETURN i LIMIT 2 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) WITH count(m) AS c "
               "OPTIONAL CALL (c) { UNWIND range(2, c) AS i RETURN i LIMIT 10 } "
               "RETURN i LIMIT 2 })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsTheBodyOfACall) {
    const Rows expected = {{"Remy", "2.000000"},
                           {"Adam", "2.000000"},
                           {"Maxime", "0.000000"},
                           {"Luc", "0.000000"},
                           {"Martina", "0.000000"},
                           {"Suhas", "0.000000"},
                           {"Cyrus", "0.000000"},
                           {"Doruk", "0.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "CALL (n) { MATCH (n)-->(m)-->(k) RETURN k LIMIT 2 } RETURN k })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "CALL (n) { MATCH (n)-->(m)-->(k) RETURN k LIMIT 2 } RETURN k })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsTheBodyOfAnOptionalCall) {
    const Rows expected = {{"Remy", "2.000000"},
                           {"Adam", "2.000000"},
                           {"Maxime", "1.000000"},
                           {"Luc", "1.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "1.000000"},
                           {"Cyrus", "1.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "OPTIONAL CALL (n) { MATCH (n)-->(m)-->(k) RETURN k LIMIT 2 } RETURN k })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "OPTIONAL CALL (n) { MATCH (n)-->(m)-->(k) RETURN k LIMIT 2 } RETURN k })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsAnOptionalMatch) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "1.000000"},
                           {"Luc", "1.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "1.000000"},
                           {"Cyrus", "1.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "OPTIONAL MATCH (n)-->(m)-->(k) RETURN k LIMIT 3 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "OPTIONAL MATCH (n)-->(m)-->(k) RETURN k LIMIT 3 })",
               expected);
}

TEST_F(LimitInWidenedFoldTest, limitsAnOptionalMatchAfterAMatch) {
    const Rows expected = {{"Remy", "3.000000"},
                           {"Adam", "3.000000"},
                           {"Maxime", "2.000000"},
                           {"Luc", "2.000000"},
                           {"Martina", "1.000000"},
                           {"Suhas", "2.000000"},
                           {"Cyrus", "2.000000"},
                           {"Doruk", "1.000000"}};

    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0.0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) OPTIONAL MATCH (m)-->(k) RETURN k LIMIT 3 })",
               expected);
    expectRows("MATCH (n:Person) "
               "RETURN n.name, reduce(acc = 0, x IN [1, 2] | acc + 0.5 * COUNT { "
               "MATCH (n)-->(m) OPTIONAL MATCH (m)-->(k) RETURN k LIMIT 3 })",
               expected);
}
