#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

class ParameterValueTypeTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();

        ParameterValue integer;
        integer.setInt64(1);
        _parameters.set("i", integer);

        ParameterValue real;
        real.setDouble(1.5);
        _parameters.set("d", real);

        ParameterValue text;
        text.setString("a");
        _parameters.set("s", text);

        ParameterValue list;
        list.setList().emplace_back().setInt64(1);
        _parameters.set("l", list);

        ParameterValue null;
        null.setNull();
        _parameters.set("n", null);

        ParameterValue map;
        ParameterValue::Map& entries = map.setMap();
        entries["name"].setString("Remy");
        entries["age"].setInt64(30);
        _parameters.set("m", map);

        ParameterValue mismatched;
        mismatched.setMap()["age"].setString("x");
        _parameters.set("mbad", mismatched);

        ParameterValue unnamed;
        unnamed.setMap()[""].setInt64(1);
        _parameters.set("mempty", unnamed);

        ParameterValue deep;
        ParameterValue::Map& deepEntries = deep.setMap();
        deepEntries["tags"].setList().emplace_back().setInt64(1);
        deepEntries["meta"].setMap()["k"].setInt64(1);
        deepEntries["none"].setNull();
        _parameters.set("mnested", deep);

        ParameterValue nested;
        ParameterValue::List& rows = nested.setList();
        rows.emplace_back().setList().emplace_back().setInt64(1);
        rows.emplace_back().setList().emplace_back().setInt64(2);
        _parameters.set("nested", nested);

        ParameterValue mixed;
        ParameterValue::List& cells = mixed.setList();
        cells.emplace_back().setInt64(1);
        cells.emplace_back().setString("a");
        cells.emplace_back().setNull();
        _parameters.set("mixed", mixed);

        ParameterValue wait;
        wait.setDuration(Duration {3'600'000'000});
        _parameters.set("wait", wait);

        ParameterValue when;
        when.setDateTime(DateTime {1'762'351'500'000'000});
        _parameters.set("when", when);
    }

protected:
    void analyzeQuery(const std::string& query) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph(), &_parameters);

        analyzer.analyze();
    }

    void expectAccepted(const std::string& query) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            ADD_FAILURE() << "query was rejected: " << query << "\nerror: " << error.what();
        }
    }

    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos) << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    ParameterMap _parameters;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ParameterValueTypeTest, anOperandIsCheckedAgainstTheTypeOfTheValue) {
    expectAccepted("RETURN $i + 1, $d * 2, $s + 'a', $l + [1], $i < $d");
    expectRejected("RETURN $s - 1", "'String' and 'Integer'");
    expectRejected("RETURN $i || 'a'", "'||'");
}

TEST_F(ParameterValueTypeTest, aListParameterHoldsElementsOfAnyType) {
    expectAccepted("UNWIND $l AS x RETURN x");
    expectAccepted("RETURN $l[0], size($l), $l + $l");
}

TEST_F(ParameterValueTypeTest, aNullIsReadAsTheNullLiteral) {
    expectAccepted("RETURN $n + 1, $n = 'a', $n IS NULL");
}

TEST_F(ParameterValueTypeTest, anUnboundParameterIsRejected) {
    expectRejected("RETURN $missing + 1", "Parameter $missing is not defined");
}

TEST_F(ParameterValueTypeTest, aSelectionTakesTheTypeOfTheValue) {
    expectAccepted("RETURN CASE WHEN true THEN $i ELSE 1 END * 2");
    expectRejected("RETURN CASE WHEN true THEN $i ELSE null END || 'a'", "'||'");
    expectAccepted("RETURN coalesce($i, 1) * 2");
    expectAccepted("RETURN $i AS x UNION RETURN 1 AS x");
    expectAccepted("RETURN reduce(acc = $i, n IN [1, 2] | acc + n) * 2");
}

TEST_F(ParameterValueTypeTest, aVariableTakesTheTypeOfTheValue) {
    expectAccepted("WITH $i AS x RETURN x * 2");
    expectRejected("WITH $i AS x RETURN x || 'a'", "'||'");
    expectAccepted("RETURN $i - 1 AS x NEXT RETURN x * 2 AS y");
    expectRejected("RETURN $i - 1 AS x NEXT RETURN x || 'a' AS y", "'||'");
}

TEST_F(ParameterValueTypeTest, aPropertyWriteTakesTheTypeOfTheValue) {
    expectAccepted("CREATE (:T {x: $i})");
    expectAccepted("MATCH (n:Person) SET n.age = $i");
    expectAccepted("CREATE (:T {x: $i}) NEXT MATCH (t:T) RETURN t.x * 2");
}

TEST_F(ParameterValueTypeTest, aMapParameterAnswersItsKeys) {
    expectAccepted("RETURN $m.name + '!', $m['age'] + 1, keys($m), size(keys($m))");
    expectAccepted("UNWIND keys($m) AS key RETURN key + '!'");
    expectAccepted("UNWIND $m AS whole RETURN whole.name");
    expectRejected("RETURN $m + 1", "'Map' and 'Integer'");
}

TEST_F(ParameterValueTypeTest, aMapParameterWritesItsEntriesAsProperties) {
    expectAccepted("MATCH (p:Person) SET p += $m");
    expectAccepted("MATCH (p:Person) SET p = $m");
    expectRejected("MATCH (p:Person) SET p += $mbad", "types 'Int64' and 'String' are incompatible");
}

TEST_F(ParameterValueTypeTest, aMapParameterNamesItsProperties) {
    expectRejected("MATCH (p:Person) SET p += $mempty", "A property name cannot be empty");
    expectAccepted("MATCH (p:Person) SET p += $mnested");
}

TEST_F(ParameterValueTypeTest, aParameterWritesEveryKindOfProperty) {
    expectAccepted("CREATE (n:Thing {v: $i}) RETURN n");
    expectAccepted("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) CREATE (a)-[:T {w: $i}]->(b)");
    expectAccepted("MATCH (n:Person) SET n.fresh = $i");
    expectRejected("MATCH (n:Person) SET n.age = $d", "'Int64' and 'Double'");
}

TEST_F(ParameterValueTypeTest, aParameterFlowsThroughWithAndCall) {
    expectAccepted("WITH $i - 1 AS x CALL (x) { RETURN x * 2 AS y } RETURN y");
    expectRejected("WITH $i - 1 AS x CALL (x) { RETURN x || 'a' AS y } RETURN y", "'||'");
}

TEST_F(ParameterValueTypeTest, twoWritesOfANewPropertyMustAgree) {
    expectRejected("CREATE (:Thing {v: $i}), (:Thing {v: $s})", "already exists with a different type");
    expectRejected("MATCH (p:Person) SET p += $mnested SET p.tags = 'x'", "'List' and 'String'");
}

TEST_F(ParameterValueTypeTest, aNestedListParameterUnwindsTwice) {
    expectAccepted("UNWIND $nested AS row UNWIND row AS x RETURN x + 1");
    expectAccepted("RETURN $nested[0][0] + 1, size($nested[1])");
}

TEST_F(ParameterValueTypeTest, aHeterogeneousListParameterHandsOutTaggedCells) {
    expectAccepted("UNWIND $mixed AS cell RETURN cell + 1, cell IS NULL, toString(cell)");
    expectAccepted("RETURN $mixed[0] + 1, $mixed[1] + 'b', size($mixed)");
}

TEST_F(ParameterValueTypeTest, aDurationOrADateTimeParameterComputesWithTime) {
    expectAccepted("RETURN $wait + duration({hours: 1}), $wait - $wait");
    expectAccepted("RETURN $wait * 2, $wait / 2.0");
    expectAccepted("RETURN $wait.hours");
    expectAccepted("RETURN $when + $wait, $when - $wait, $when - $when");
    expectAccepted("RETURN $when.year, $when > datetime('2026-01-01')");
    expectRejected("RETURN $wait - 1", "'Duration' and 'Integer'");
    expectRejected("RETURN $when + 1", "'DateTime' and 'Integer'");
    expectRejected("RETURN $when * 2", "'DateTime' and 'Integer'");
}
