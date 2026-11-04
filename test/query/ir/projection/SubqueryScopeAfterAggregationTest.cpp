#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Every expected row is Neo4j 5.26.30's answer to the same query. After an aggregating
// projection, a pattern subquery naming a variable the projection did not keep declares
// a new variable over the whole graph rather than reading the dropped one
class SubqueryScopeAfterAggregationTest : public WriteQueryTest {
};

TEST_F(SubqueryScopeAfterAggregationTest, ordersByASubqueryNamingADroppedVariable) {
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY EXISTS { (n)-->(:Person) } DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3 RETURN name, k",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(SubqueryScopeAfterAggregationTest, ordersByASubqueryNamingTheAggregatedVariable) {
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (m)-->() } DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (m)-->() } + count(m) DESC, name LIMIT 3",
                      {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}});
}

TEST_F(SubqueryScopeAfterAggregationTest, ordersByAFullSubqueryNamingADroppedVariable) {
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { MATCH (n)-->(x) RETURN x } DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY size(COLLECT { MATCH (n)-->(x) RETURN x }) DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(SubqueryScopeAfterAggregationTest, readsTheSameSubqueryValueOnEveryRow) {
    expectRowsInOrder("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
                      "RETURN name, k, COUNT { (n)-->() } AS c ORDER BY name LIMIT 3",
                      {{"Adam", "3", "18"}, {"Cyrus", "2", "18"}, {"Doruk", "1", "18"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
                      "RETURN name, COUNT { (m)-->() } AS c, EXISTS { (n)-->(:Person) } AS e ORDER BY name LIMIT 2",
                      {{"Adam", "18", "true"}, {"Cyrus", "18", "true"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) WITH n.name AS name, count(m) AS k "
                      "RETURN name, COUNT { (n:Person {name: 'Remy'})-->() } AS c ORDER BY name LIMIT 2",
                      {{"Adam", "4"}, {"Cyrus", "4"}});
}

TEST_F(SubqueryScopeAfterAggregationTest, ordersPerGroupThroughAReturnedAlias) {
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (x:Person {name: name})-->() } DESC, name LIMIT 3",
                      {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (n:Person {name: name})-->() } DESC LIMIT 2",
                      {{"Remy", "4"}, {"Adam", "3"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (p:Person)-->() } DESC, name LIMIT 3",
                      {{"Adam", "3"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(SubqueryScopeAfterAggregationTest, ordersPerGroupThroughAKeptVariable) {
    expectRowsInOrder("MATCH (n:Person)-->(m) RETURN n, n.name AS name, count(m) AS k "
                      "ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3",
                      {{"0", "Remy", "4"}, {"1", "Adam", "3"}, {"15", "Cyrus", "2"}});
    expectRowsInOrder("MATCH (n:Person)-->(m) WITH n, count(m) AS k "
                      "RETURN n.name AS name, k ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3",
                      {{"Remy", "4"}, {"Adam", "3"}, {"Cyrus", "2"}});
    expectRowsInOrder("MATCH (n:Person) RETURN n.name AS name ORDER BY COUNT { (n)-->() } DESC, name LIMIT 3",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}});
}
