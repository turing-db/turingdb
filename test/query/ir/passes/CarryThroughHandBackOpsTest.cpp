#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "QueryStatus.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An OPTIONAL MATCH and a CALL hand back the columns they carry into their body, so a body
// yielding them back carries each one up to its yield. A column only handed back, which nothing
// past the op reads, leaves the op, its body and every carry between. A value computed once per
// row in front of the op reaches a body repeating rows through the same carries. Remy and Adam
// are the two people aged 32 and know each other well; no other person has an age.
class CarryThroughHandBackOpsTest : public WriteQueryTest {
protected:
    struct ReadCount {
        size_t _inBody {0};
        size_t _total {0};
    };

    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void generateProgram(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module, std::string& program) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        llvm::raw_string_ostream stream(program);
        moduleOp.print(stream);
    }

    void nlProgramOf(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();
        ASSERT_EQ(sink.rows().size(), 1u) << query;

        program = sink.rows().front().back();
    }

    // The values named in the line of @param program holding @param text, between the first
    // @param open after it and the @param close ending them
    static void expectNamedValues(std::string_view program, std::string_view text, char open, char close, size_t expected) {
        const size_t textPosition = program.find(text);
        ASSERT_NE(textPosition, std::string_view::npos) << text << "\n" << program;

        const size_t lineEnd = std::min(program.find('\n', textPosition), program.size());
        const size_t openPosition = program.find(open, textPosition + text.size() - 1);
        const size_t closePosition = program.find(close, openPosition);
        ASSERT_LT(closePosition, lineEnd) << text << "\n" << program;

        const std::string_view named = program.substr(openPosition, closePosition - openPosition);
        EXPECT_EQ(static_cast<size_t>(std::count(named.begin(), named.end(), '%')), expected) << text << "\n" << program;
    }

    // The reads of @param property in the db program of @param query, and how many of them sit
    // in the body of a @param BodyOp
    template <typename BodyOp>
    void expectDBNodePropertyReads(std::string_view query, llvm::StringRef property, size_t inBody, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        ReadCount count;
        module->walk([property, &count](mlir::db::GetNodeProperties read) {
            if (read.getProperty() != property) {
                return;
            }

            count._total++;
            if (read->getParentOfType<BodyOp>()) {
                count._inBody++;
            }
        });

        EXPECT_EQ(count._inBody, inBody) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    // The reads of @param property in the nl program of @param query, and how many of them sit
    // deeper than the body of the first loop, the one over the scanned rows
    void expectNLNodePropertyReads(std::string_view query, std::string_view property, size_t nested, size_t total) {
        std::string program;
        ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, program));

        const std::string definition = " = nl.get_property_type(\"" + std::string(property) + "\")";
        const size_t definitionPosition = program.find(definition);
        ASSERT_NE(definitionPosition, std::string::npos) << property << "\n" << program;

        const size_t valueStart = program.rfind('%', definitionPosition);
        const std::string readSuffix = ", " + program.substr(valueStart, definitionPosition - valueStart) + ")";

        size_t rowBodyIndent = std::string_view::npos;
        size_t lineStart = 0;
        ReadCount count;

        while (lineStart < program.size()) {
            const size_t lineEnd = std::min(program.find('\n', lineStart), program.size());
            const std::string_view line = std::string_view(program).substr(lineStart, lineEnd - lineStart);
            lineStart = lineEnd + 1;

            const size_t indent = line.find_first_not_of(' ');
            if (indent == std::string_view::npos) {
                continue;
            }

            const std::string_view text = line.substr(indent);
            if (rowBodyIndent == std::string_view::npos && text.starts_with("nl.for ")) {
                rowBodyIndent = indent + 2;
            }

            const bool readsTheProperty = text.find("nl.get_node_properties(") != std::string_view::npos
                                       && text.find(readSuffix) != std::string_view::npos;
            if (!readsTheProperty) {
                continue;
            }

            count._total++;
            if (indent > rowBodyIndent) {
                count._inBody++;
            }
        }

        EXPECT_EQ(count._inBody, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    // The columns @param query carries into its one @param RegionOp
    template <typename RegionOp>
    void expectInputs(std::string_view query, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        size_t count = 0;
        size_t inputs = 0;
        module->walk([&count, &inputs](RegionOp op) {
            count++;
            inputs = op.getInputColumns().size();
        });

        ASSERT_EQ(count, 1u) << query << "\n" << program;
        EXPECT_EQ(inputs, expected) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

// The pattern walks from n alone; i and the edge it was matched over are only handed back
TEST_F(CarryThroughHandBackOpsTest, dropsTheInputsAnOptionalMatchOnlyHandsBack) {
    const std::string_view query = "MATCH (n:Person)-->(i:Interest) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(k) RETURN k.name";
    expectInputs<mlir::db::OptionalMatch>(query, 1);

    std::string nlProgram;
    ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, nlProgram));
    expectNamedValues(nlProgram, "nl.optional_buffer(", '(', ')', 1);
    expectNamedValues(nlProgram, "nl.get_out_edges_by_type(", '{', '}', 1);

    expectRows(query, {{"Adam"}, {"Adam"}, {"Adam"}, {"Remy"}, {"Remy"}, {"null"}, {"null"}, {"null"},
                       {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(CarryThroughHandBackOpsTest, keepsTheInputAnOptionalMatchHandsBackToAReader) {
    const std::string_view query = "MATCH (n:Person)-->(i:Interest) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(k) RETURN i.name, k.name";
    expectInputs<mlir::db::OptionalMatch>(query, 2);

    expectRows(query, {{"Ghosts", "Adam"}, {"Computers", "Adam"}, {"Eighties", "Adam"}, {"Bio", "Remy"},
                       {"Cooking", "Remy"}, {"Bio", "null"}, {"Padel", "null"}, {"Animals", "null"},
                       {"Computers", "null"}, {"Cooking", "null"}, {"Gym", "null"}, {"JiuJitsu", "null"},
                       {"Gym", "null"}, {"Travel", "null"}, {"Gym", "null"}});
}

// The body walks from n alone; f and the edge to it are only yielded back
TEST_F(CarryThroughHandBackOpsTest, dropsTheInputsACallBodyOnlyYieldsBack) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "CALL { WITH n MATCH (n)-->(k:Interest) RETURN k } "
                                   "RETURN k.name";
    expectInputs<mlir::db::CallSubquery>(query, 1);

    std::string nlProgram;
    ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, nlProgram));
    expectNamedValues(nlProgram, "nl.get_out_edges_by_label(", '{', '}', 1);

    expectRows(query, {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}});
}

TEST_F(CarryThroughHandBackOpsTest, keepsTheInputACallBodyYieldsBackToAReader) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "CALL { WITH n MATCH (n)-->(k:Interest) RETURN k } "
                                   "RETURN f.name, k.name";
    expectInputs<mlir::db::CallSubquery>(query, 2);

    expectRows(query, {{"Adam", "Ghosts"}, {"Adam", "Computers"}, {"Adam", "Eighties"}, {"Remy", "Bio"},
                       {"Remy", "Cooking"}});
}

// i and the edge come into the CALL and on into the OPTIONAL MATCH in its body, which hands them
// back to the CALL's yield
TEST_F(CarryThroughHandBackOpsTest, dropsTheInputsAnOptionalMatchInACallBodyOnlyHandsBack) {
    const std::string_view query = "MATCH (n:Person)-->(i:Interest) "
                                   "CALL { WITH n OPTIONAL MATCH (n)-[:KNOWS_WELL]->(k) RETURN k } "
                                   "RETURN k.name";
    expectInputs<mlir::db::CallSubquery>(query, 1);

    mlir::OwningOpRef<mlir::ModuleOp> module;
    std::string program;
    ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

    module->walk([&program](mlir::db::OptionalMatch optional) {
        mlir::Block& callBody = optional->getParentOfType<mlir::db::CallSubquery>().getBody().front();
        for (const mlir::Value input : optional.getInputColumns()) {
            EXPECT_EQ(input, callBody.getArgument(0)) << program;
        }
    });

    expectRows(query, {{"Adam"}, {"Adam"}, {"Adam"}, {"Remy"}, {"Remy"}, {"null"}, {"null"}, {"null"},
                       {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(CarryThroughHandBackOpsTest, readsTheOuterPropertyOfAnOptionalMatchOncePerRow) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age > n.age RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::OptionalMatch>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "null"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                       {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});

    const std::string_view matched = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age >= n.age RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::OptionalMatch>(matched, "age", 1, 2);

    expectRows(matched, {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                         {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

// A row the pattern missed comes back with n and its age as they came in, and m null
TEST_F(CarryThroughHandBackOpsTest, handsBackTheOuterPropertyOnARowAnOptionalMatchMissed) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age >= n.age "
                                   "RETURN n.name, n.age, m.name";
    expectDBNodePropertyReads<mlir::db::OptionalMatch>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "32", "Adam"}, {"Adam", "32", "Remy"}, {"Maxime", "null", "null"},
                       {"Luc", "null", "null"}, {"Martina", "null", "null"}, {"Suhas", "null", "null"},
                       {"Cyrus", "null", "null"}, {"Doruk", "null", "null"}});
}

TEST_F(CarryThroughHandBackOpsTest, readsTheOuterPropertyOfACallBodyOncePerRow) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(m) WHERE m.age > n.age RETURN m } "
                                   "RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::CallSubquery>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);
    expectRows(query, {});

    const std::string_view matched = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(m) WHERE m.age >= n.age RETURN m } "
                                     "RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::CallSubquery>(matched, "age", 1, 2);

    expectRows(matched, {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// A person the body finds no m for comes back once, with n as it came in and m null
TEST_F(CarryThroughHandBackOpsTest, readsTheOuterPropertyOfAnOptionalCallBodyOncePerRow) {
    const std::string_view query = "MATCH (n:Person) "
                                   "OPTIONAL CALL { WITH n MATCH (n)-->(m) WHERE m.age >= n.age RETURN m } "
                                   "RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::CallSubquery>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                       {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(CarryThroughHandBackOpsTest, dropsTheInputsAnOptionalCallBodyOnlyYieldsBack) {
    const std::string_view query = "MATCH (n:Person)-[:KNOWS_WELL]->(f) "
                                   "OPTIONAL CALL { WITH n MATCH (n)-->(k:Interest) RETURN k } "
                                   "RETURN k.name";
    expectInputs<mlir::db::CallSubquery>(query, 1);

    expectRows(query, {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}});
}

// The CALL runs once per person, so the age read out of the OPTIONAL MATCH stays in its body
TEST_F(CarryThroughHandBackOpsTest, readsTheOuterPropertyOfAnOptionalMatchInACallBodyOncePerRow) {
    const std::string_view query = "MATCH (n:Person) "
                                   "CALL { WITH n OPTIONAL MATCH (n)-->(m) WHERE m.age >= n.age RETURN m } "
                                   "RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::OptionalMatch>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                       {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

// Moving the read out of a body that writes would change what the write sees
TEST_F(CarryThroughHandBackOpsTest, keepsTheBodyOfACallThatWrites) {
    const std::string_view query = "MATCH (n:Person) "
                                   "CALL { WITH n MATCH (n)-->(m) WHERE m.age >= n.age CREATE (:Visit {name: m.name}) RETURN m } "
                                   "RETURN n.name, m.name";
    expectDBNodePropertyReads<mlir::db::CallSubquery>(query, "age", 2, 2);

    expectWriteRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}});
    expectRows("MATCH (v:Visit) RETURN v.name", {{"Adam"}, {"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
