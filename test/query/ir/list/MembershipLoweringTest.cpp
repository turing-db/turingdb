#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A membership test over a typed list keeps the typed db.in and nl.in it is built as, and one
// over a type-erased cell reads the cell column as it stands.
class MembershipLoweringTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void expectSignature(std::string_view stage, std::string_view op, std::string_view query, std::string_view signature) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              "EXPLAIN (" + std::string(stage) + ") " + std::string(query),
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const std::string_view dump = dumpOf(sink, stage);
        const size_t opAt = dump.find(op);
        ASSERT_NE(opAt, std::string_view::npos) << dump;

        const std::string_view line = dump.substr(opAt, dump.find('\n', opAt) - opAt);
        const size_t signatureAt = line.find(" : ");
        ASSERT_NE(signatureAt, std::string_view::npos) << line;

        EXPECT_EQ(line.substr(signatureAt + 3), signature) << line;
    }

    static std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MembershipLoweringTest, keepsTheTypedMembershipOfAPropertyInAList) {
    constexpr std::string_view QUERY = "MATCH (p:Person) WHERE p.age IN [32, 33] RETURN p.name";

    expectSignature("codegen",
                    "db.in",
                    QUERY,
                    "(!db.column<!storage.nullable<i64>>, !db.column<!storage.list<i64>>) -> !db.column<!storage.bool>");
    expectSignature("nl",
                    "nl.in",
                    QUERY,
                    "(!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<i64>>) -> !nl.chunk<!storage.nullable<i1>>");
}

TEST_F(MembershipLoweringTest, keepsTheTypedMembershipOfAStringInAList) {
    constexpr std::string_view QUERY = "UNWIND ['a', 'b'] AS s RETURN s IN ['a', 'c']";

    expectSignature("codegen",
                    "db.in",
                    QUERY,
                    "(!db.column<!storage.string>, !db.column<!storage.list<!storage.string>>) -> !db.column<!storage.bool>");
    expectSignature("nl",
                    "nl.in",
                    QUERY,
                    "(!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>");
}

TEST_F(MembershipLoweringTest, readsTheCellColumnOfAMembershipInACell) {
    constexpr std::string_view QUERY = "UNWIND [[1], 1] AS x RETURN 1 IN x";

    expectSignature("nl", "nl.in", QUERY, "(!nl.chunk<i64>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.nullable<i1>>");
}
