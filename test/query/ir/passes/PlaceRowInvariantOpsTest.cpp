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
#include "llvm/ADT/STLFunctionalExtras.h"
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

// A value computed from the columns in front of an op repeating rows - a hop, a walk, a product,
// an unwind, a body run per element - is computed once in front of it and carried through when a
// use of it comes before any filter after the op, and after the last filter all its uses follow
// otherwise. Remy and Adam are the two people aged 32; no other person has an age.
class PlaceRowInvariantOpsTest : public WriteQueryTest {
protected:
    struct OpCount {
        size_t _placed {0};
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

    static bool readsNodeProperty(mlir::Operation* op, llvm::StringRef property) {
        mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
        return read && read.getProperty() == property;
    }

    // The ops of the db program of @param query that @param isCounted picks, and how many of
    // them sit in a region of a @param BodyOp
    template <typename BodyOp>
    void expectDBOpsInside(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> isCounted, size_t inside, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        OpCount count;
        module->walk([&isCounted, &count](mlir::Operation* op) {
            if (!isCounted(op)) {
                return;
            }

            count._total++;
            if (op->getParentOfType<BodyOp>()) {
                count._placed++;
            }
        });

        EXPECT_EQ(count._placed, inside) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    // The ops of the db program of @param query that @param isCounted picks, and how many of
    // them come before the first @param MarkerOp
    template <typename MarkerOp>
    void expectDBOpsBefore(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> isCounted, size_t before, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        OpCount count;
        bool markerSeen = false;
        module->walk<mlir::WalkOrder::PreOrder>([&isCounted, &count, &markerSeen](mlir::Operation* op) {
            if (mlir::isa<MarkerOp>(op)) {
                markerSeen = true;
            }

            if (!isCounted(op)) {
                return;
            }

            count._total++;
            if (!markerSeen) {
                count._placed++;
            }
        });

        EXPECT_TRUE(markerSeen) << query << "\n" << program;
        EXPECT_EQ(count._placed, before) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    template <typename BodyOp>
    void expectDBReadsInside(std::string_view query, llvm::StringRef property, size_t inside, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            return readsNodeProperty(op, property);
        };

        expectDBOpsInside<BodyOp>(query, readsTheProperty, inside, total);
    }

    template <typename MarkerOp>
    void expectDBReadsBefore(std::string_view query, llvm::StringRef property, size_t before, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            return readsNodeProperty(op, property);
        };

        expectDBOpsBefore<MarkerOp>(query, readsTheProperty, before, total);
    }

    template <typename CountedOp, typename BodyOp>
    void expectDBOpsOfInside(std::string_view query, size_t inside, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        expectDBOpsInside<BodyOp>(query, isCounted, inside, total);
    }

    template <typename CountedOp, typename MarkerOp>
    void expectDBOpsOfBefore(std::string_view query, size_t before, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        expectDBOpsBefore<MarkerOp>(query, isCounted, before, total);
    }

    void nlProgramOf(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();
        ASSERT_EQ(sink.rows().size(), 1u) << query;

        program = sink.rows().front().back();
    }

    // The reads of @param property in the nl program of @param query. @param marker set, how
    // many of them come before the first line holding it; otherwise how many of them sit deeper
    // than the body of the first loop, the one over the scanned rows.
    void countNLReads(std::string_view query, std::string_view property, std::string_view marker, OpCount& count, std::string& program) {
        ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, program));

        const std::string definition = " = nl.get_property_type(\"" + std::string(property) + "\")";
        const size_t definitionPosition = program.find(definition);
        ASSERT_NE(definitionPosition, std::string::npos) << property << "\n" << program;

        const size_t valueStart = program.rfind('%', definitionPosition);
        const std::string readSuffix = ", " + program.substr(valueStart, definitionPosition - valueStart) + ")";

        size_t rowBodyIndent = std::string_view::npos;
        bool markerSeen = false;
        size_t lineStart = 0;

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

            if (!marker.empty() && text.find(marker) != std::string_view::npos) {
                markerSeen = true;
            }

            const bool readsTheProperty = text.find("nl.get_node_properties(") != std::string_view::npos
                                       && text.find(readSuffix) != std::string_view::npos;
            if (!readsTheProperty) {
                continue;
            }

            count._total++;

            const bool placed = marker.empty() ? indent > rowBodyIndent : !markerSeen;
            if (placed) {
                count._placed++;
            }
        }
    }

    void expectNLReadsBefore(std::string_view query, std::string_view property, std::string_view marker, size_t before, size_t total) {
        OpCount count;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(countNLReads(query, property, marker, count, program));

        EXPECT_NE(program.find(marker), std::string::npos) << query << "\n" << program;
        EXPECT_EQ(count._placed, before) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    void expectNLReadsNested(std::string_view query, std::string_view property, size_t nested, size_t total) {
        OpCount count;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(countNLReads(query, property, {}, count, program));

        EXPECT_EQ(count._placed, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

// The comparison is the use of n.age and comes before the filter, so the age is read once for
// Remy in front of the hop and carried to each of his 4 edges
TEST_F(PlaceRowInvariantOpsTest, readsTheSeedPropertyAFilterComparesInFrontOfTheHop) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-->(m) WHERE m.age > n.age RETURN m.name";
    expectDBReadsBefore<mlir::db::GetOutEdges>(query, "age", 1, 2);
    expectNLReadsBefore(query, "age", "nl.get_out_edges(", 1, 2);
    expectRows(query, {});

    expectRows("MATCH (n:Person {name: 'Remy'})-->(m) WHERE m.age >= n.age RETURN m.name", {{"Adam"}});
}

TEST_F(PlaceRowInvariantOpsTest, readsTheSeedPropertyAFilterComparesInFrontOfTheWalk) {
    const std::string_view query = "MATCH (a:Person {name: 'Remy'})-[*1..2]->(b) WHERE b.age >= a.age RETURN b.name";
    expectDBReadsBefore<mlir::db::ExplorePaths>(query, "age", 1, 2);
    expectNLReadsBefore(query, "age", "nl.explore_paths(", 1, 2);
    expectRows(query, {{"Adam"}, {"Remy"}, {"Remy"}});
}

// n.age is read for the comparison and for the projection after the filter: one read, in front
// of the hop
TEST_F(PlaceRowInvariantOpsTest, readsAValueUsedBeforeAndAfterTheFilterInFrontOfTheHop) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-->(m) WHERE m.age >= n.age RETURN n.age, m.name";
    expectDBReadsBefore<mlir::db::GetOutEdges>(query, "age", 1, 2);
    expectNLReadsBefore(query, "age", "nl.get_out_edges(", 1, 2);
    expectRows(query, {{"32", "Adam"}});

    const std::string_view comprehension = "MATCH (n:Person) RETURN [(n)-->(m) WHERE m.age >= n.age | n.age]";
    expectDBReadsInside<mlir::db::PatternComprehension>(comprehension, "age", 1, 2);
    expectRows(comprehension, {{"[32]"}, {"[32]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}});
}

// The name of n is used by the projection alone, after the filter, so it is read for the edges
// the filter keeps rather than once per person and carried through every edge
TEST_F(PlaceRowInvariantOpsTest, readsTheOuterPropertyOfAPatternComprehensionAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person) RETURN [(n)-->(m) WHERE m.age > 30 | n.name]";
    expectDBReadsInside<mlir::db::PatternComprehension>(query, "name", 1, 1);
    expectNLReadsNested(query, "name", 1, 1);
    expectRows(query, {{"[Remy]"}, {"[Adam]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}});

    const std::string_view walk = "MATCH (n:Person) RETURN [(n)-[*1..2]->(m) WHERE m.age > 30 | n.name]";
    expectDBReadsInside<mlir::db::PatternComprehension>(walk, "name", 1, 1);
    expectRows(walk, {{"[Remy, Remy, Remy]"}, {"[Adam, Adam]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}});
}

// No filter follows the hop, so the inner list is built once per person; behind a filter it is
// built for each edge the filter keeps, out of an age read there
TEST_F(PlaceRowInvariantOpsTest, buildsAnInnerListInFrontOfTheHopOnlyWhereNoFilterFollowsIt) {
    const std::string_view unfiltered = "MATCH (n:Person) RETURN [(n)-->(m) | [x IN [1, 2] | n.age]]";
    expectDBOpsOfInside<mlir::db::ListComprehension, mlir::db::PatternComprehension>(unfiltered, 0, 1);
    expectDBReadsInside<mlir::db::PatternComprehension>(unfiltered, "age", 0, 1);

    const std::string_view filtered = "MATCH (n:Person) RETURN [(n)-->(m) WHERE m.name > 'C' | [x IN [1, 2] | n.age]]";
    expectDBOpsOfInside<mlir::db::ListComprehension, mlir::db::PatternComprehension>(filtered, 1, 1);
    expectDBReadsInside<mlir::db::PatternComprehension>(filtered, "age", 1, 1);
    expectDBReadsInside<mlir::db::ListComprehension>(filtered, "age", 0, 1);

    expectRows(filtered, {{"[[32, 32], [32, 32], [32, 32]]"}, {"[[32, 32], [32, 32]]"}, {"[[null, null]]"},
                          {"[[null, null]]"}, {"[[null, null]]"}, {"[[null, null], [null, null]]"},
                          {"[[null, null], [null, null]]"}, {"[[null, null]]"}});
}

// The comparison pairs every edge with every interest. Each name is read in its own factor,
// once per edge and once per interest, in front of the product.
TEST_F(PlaceRowInvariantOpsTest, readsEachSideOfAProductInItsOwnFactor) {
    const std::string_view query = "MATCH (n:Person) RETURN COUNT { MATCH (n)-->(m), (k:Interest) WHERE k.name > m.name }";
    expectDBReadsInside<mlir::db::CrossProduct>(query, "name", 2, 2);
    expectNLReadsBefore(query, "name", "nl.cross_product", 2, 2);
    expectRows(query, {{"26"}, {"15"}, {"9"}, {"16"}, {"6"}, {"5"}, {"3"}, {"3"}});

    const std::string_view unwound = "MATCH (n:Person) UNWIND [1, 2] AS x RETURN n.name, x";
    expectDBReadsInside<mlir::db::CrossProduct>(unwound, "name", 1, 1);
    expectRows(unwound, {{"Remy", "1"}, {"Remy", "2"}, {"Adam", "1"}, {"Adam", "2"}, {"Maxime", "1"}, {"Maxime", "2"},
                         {"Luc", "1"}, {"Luc", "2"}, {"Martina", "1"}, {"Martina", "2"}, {"Suhas", "1"}, {"Suhas", "2"},
                         {"Cyrus", "1"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Doruk", "2"}});
}

// The projection of n.age before the filter is used only after it, so the age is read for the
// rows the filter keeps
TEST_F(PlaceRowInvariantOpsTest, readsAValueEveryUseOfWhichFollowsAFilterAfterIt) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-->(m) WITH n.age AS a, m WHERE m.name > 'C' RETURN a";
    expectDBReadsBefore<mlir::db::FilterOp>(query, "age", 0, 1);
    expectNLReadsBefore(query, "age", "nl.filter", 0, 1);
    expectRows(query, {{"32"}, {"32"}, {"32"}});
}

// Every use of n.age follows the filter and the limit, so it is read after the limit
TEST_F(PlaceRowInvariantOpsTest, readsAValueEveryUseOfWhichFollowsTwoFiltersAfterTheLast) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-->(m) WITH n.age AS a, m WHERE m.name > 'C' "
                                   "WITH a LIMIT 1 RETURN a";
    expectDBReadsBefore<mlir::db::Limit>(query, "age", 0, 1);
    expectRows(query, {{"32"}});

    const std::string_view twoHops = "MATCH (n:Person) RETURN [(n)-->(m)-->(k) WHERE m.age > 30 AND k.age > 30 | n.name]";
    expectDBReadsInside<mlir::db::PatternComprehension>(twoHops, "name", 1, 1);
    expectRows(twoHops, {{"[Remy]"}, {"[Adam]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}, {"[]"}});
}

// Remy is 32, so the division raises on each of his edges. It stays after the hop and before
// the filter: nothing divides for a node with no edge, and every edge of Remy divides.
TEST_F(PlaceRowInvariantOpsTest, keepsADivisionThatMayRaiseWhereItIs) {
    applyWrite("CREATE (:Lonely {name: 'Lonely', age: 32})");

    const std::string_view lonely = "MATCH (n:Lonely {name: 'Lonely'})-->(m) WHERE m.age > 10 / (n.age - 32) RETURN m.name";
    expectDBOpsOfBefore<mlir::db::DivOp, mlir::db::GetOutEdges>(lonely, 0, 1);
    expectDBReadsBefore<mlir::db::GetOutEdges>(lonely, "age", 1, 2);
    expectRows(lonely, {});

    expectError("MATCH (n:Person {name: 'Remy'})-->(m) WHERE m.age > 10 / (n.age - 32) RETURN m.name",
                "Attempted to divide by zero");

    const std::string_view projected = "MATCH (n:Person {name: 'Remy'})-->(m) WITH n, m, 10 / (n.age - 32) AS q "
                                       "WHERE m.age > 100 RETURN q";
    expectDBOpsOfBefore<mlir::db::DivOp, mlir::db::FilterOp>(projected, 1, 1);
    expectError(projected, "Attempted to divide by zero");
}

// Every use of the age follows the filter, but the SET between writes it, so it is read where it
// is: after the hop and in front of the SET
TEST_F(PlaceRowInvariantOpsTest, keepsAReadInFrontOfAWriteAfterIt) {
    const std::string_view query = "MATCH (n:Person {name: 'Remy'})-->(m) WITH n, m, n.age AS a SET n.age = 1 "
                                   "WITH a, m WHERE m.name > 'C' RETURN a";
    expectDBReadsBefore<mlir::db::GetOutEdges>(query, "age", 0, 1);
    expectDBReadsBefore<mlir::db::SetNodeProperty>(query, "age", 1, 1);
    expectWriteRows(query, {{"32"}, {"32"}, {"32"}});
}

// A CALL body and an OPTIONAL CALL body read n.name after their filter, for the rows it keeps
TEST_F(PlaceRowInvariantOpsTest, readsTheOuterPropertyOfACallBodyAfterTheFilter) {
    const std::string_view call = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(m) WHERE m.age > 30 RETURN n.name AS nm } RETURN nm";
    expectDBReadsInside<mlir::db::CallSubquery>(call, "name", 1, 1);
    expectRows(call, {{"Remy"}, {"Adam"}});

    const std::string_view optionalCall = "MATCH (n:Person) "
                                          "OPTIONAL CALL { WITH n MATCH (n)-->(m) WHERE m.age > 30 RETURN n.name AS nm } "
                                          "RETURN nm";
    expectDBReadsInside<mlir::db::CallSubquery>(optionalCall, "name", 1, 1);
    expectRows(optionalCall, {{"Remy"}, {"Adam"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});

    const std::string_view compared = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(m) WHERE m.age >= n.age RETURN m } "
                                      "RETURN m.name";
    expectDBReadsInside<mlir::db::CallSubquery>(compared, "age", 1, 2);
    expectRows(compared, {{"Adam"}, {"Remy"}});
}

// The WHERE of an OPTIONAL MATCH compares n.age, read once per person in front of it. The
// pattern comprehension in the other WHERE uses n.name after its own filter only.
TEST_F(PlaceRowInvariantOpsTest, placesTheReadsOfAnOptionalMatchByTheSameRule) {
    const std::string_view compared = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age >= n.age RETURN m.name";
    expectDBReadsInside<mlir::db::OptionalMatch>(compared, "age", 1, 2);
    expectRows(compared, {{"Adam"}, {"Remy"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});

    const std::string_view projected = "MATCH (n:Person) "
                                       "OPTIONAL MATCH (n)-->(m) WHERE size([(m)-->(k) WHERE k.age > 30 | n.name]) > 0 "
                                       "RETURN m.name";
    expectDBReadsInside<mlir::db::PatternComprehension>(projected, "name", 1, 2);
    expectRows(projected, {{"Adam"}, {"Ghosts"}, {"Remy"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"},
                           {"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
