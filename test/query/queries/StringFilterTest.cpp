#include <gtest/gtest.h>

#include <string_view>
#include <set>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/Dataframe.h"
#include "writers/GraphWriter.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"
#include "QueryStatus.h"

using namespace turing::test;

// =============================================================================
// STRING FILTER TESTS
// Tests for filtering on string properties in queries.
// =============================================================================

namespace {

// Test graph for string filter tests
class StringFilterTestGraph {
public:
    static void createGraph(Graph* graph) {
        GraphWriter writer{graph};
        writer.setName("stringfiltertest");

        // =====================================================================
        // PERSONS: A, B, C, D, E, F
        // A, B: isFrench=true
        // C, D: isFrench=false
        // E: isFrench is NULL (missing property)
        // F: isolated (no edges)
        // =====================================================================
        auto A = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(A, "name", "A");
        writer.addNodeProperty<types::Bool>(A, "isFrench", true);

        auto B = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(B, "name", "B");
        writer.addNodeProperty<types::Bool>(B, "isFrench", true);

        auto C = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(C, "name", "C");
        writer.addNodeProperty<types::Bool>(C, "isFrench", false);

        auto D = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(D, "name", "D");
        writer.addNodeProperty<types::Bool>(D, "isFrench", false);

        auto E = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(E, "name", "E");
        // E has NO isFrench property - null case

        auto F = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(F, "name", "F");
        writer.addNodeProperty<types::Bool>(F, "isFrench", false);
        // F has no edges - isolated node

        // =====================================================================
        // INTERESTS: I1 (Shared), I2 (Unique), I3 (null name), I4 (Orphan), I5 (MegaHub)
        // I1: connected to A, B, C (3 persons - medium fan-in)
        // I2: connected only to A (single source)
        // I3: has NULL name property
        // I4: no incoming edges (orphan)
        // I5: connected to A, B, C, D, E (5 persons - high fan-in)
        // =====================================================================
        auto I1 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I1, "name", "Shared");

        auto I2 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I2, "name", "Unique");

        [[maybe_unused]] auto I3 = writer.addNode({"Interest"});
        // I3 has NO name property - null case

        auto I4 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I4, "name", "Orphan");
        // I4 has no incoming edges

        auto I5 = writer.addNode({"Interest"});
        writer.addNodeProperty<types::String>(I5, "name", "MegaHub");

        // =====================================================================
        // INTERESTED_IN edges: Person -> Interest
        // =====================================================================
        writer.addEdge("INTERESTED_IN", A, I1);  // A -> Shared
        writer.addEdge("INTERESTED_IN", B, I1);  // B -> Shared
        writer.addEdge("INTERESTED_IN", C, I1);  // C -> Shared
        writer.addEdge("INTERESTED_IN", A, I2);  // A -> Unique (only A)
        writer.addEdge("INTERESTED_IN", A, I5);  // A -> MegaHub
        writer.addEdge("INTERESTED_IN", B, I5);  // B -> MegaHub
        writer.addEdge("INTERESTED_IN", C, I5);  // C -> MegaHub
        writer.addEdge("INTERESTED_IN", D, I5);  // D -> MegaHub
        writer.addEdge("INTERESTED_IN", E, I5);  // E -> MegaHub

        writer.submit();
    }
};

} // namespace

class StringFilterTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path{_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        StringFilterTestGraph::createGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "stringfiltertest";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db{nullptr};
    Graph* _graph{nullptr};

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
        return res;
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }
};

// =============================================================================
// String property filter tests
// WHERE clause filtering on string properties
// =============================================================================

// Filter on interest name property
// Expected: Returns (A, Unique)
TEST_F(StringFilterTest, filterOnInterestName) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        WHERE b.name = 'Unique'
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;

    bool foundResult = false;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* aNames = findColumn(df, "a.name");
        auto* bNames = findColumn(df, "b.name");
        ASSERT_TRUE(aNames && bNames);

        auto* aCol = aNames->as<ColumnOptVector<String>>();
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(aCol && bCol);

        // Should have exactly 1 row: (A, Unique)
        ASSERT_EQ(aCol->size(), 1);
        EXPECT_EQ(*aCol->at(0), "A");
        EXPECT_EQ(*bCol->at(0), "Unique");
        foundResult = true;
    });
    ASSERT_TRUE(res) << "Query failed with status: "
                      << db::QueryStatusDescription::value(res.getStatus())
                      << " Error: " << res.getError();
    EXPECT_TRUE(foundResult) << "Should find result (A, Unique)";
}

// Filter on person name property
// Expected: Returns A's interests (Shared, Unique, MegaHub)
TEST_F(StringFilterTest, filterOnPersonName) {
    constexpr std::string_view QUERY = R"(
        MATCH (a:Person)-->(b:Interest)
        WHERE a.name = 'A'
        RETURN a.name, b.name
    )";

    using String = types::String::Primitive;

    std::set<String> foundInterests;
    auto res = query(QUERY, [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* bNames = findColumn(df, "b.name");
        ASSERT_TRUE(bNames);
        auto* bCol = bNames->as<ColumnOptVector<String>>();
        ASSERT_TRUE(bCol);

        for (size_t i = 0; i < bCol->size(); i++) {
            if (bCol->at(i)) {
                foundInterests.insert(*bCol->at(i));
            }
        }
    });
    ASSERT_TRUE(res) << "Query failed with status: "
                      << db::QueryStatusDescription::value(res.getStatus())
                      << " Error: " << res.getError();
    // A is connected to: Shared, Unique, MegaHub
    EXPECT_EQ(foundInterests.size(), 3) << "Person A should have 3 interests";
    EXPECT_TRUE(foundInterests.count("Shared")) << "Missing interest: Shared";
    EXPECT_TRUE(foundInterests.count("Unique")) << "Missing interest: Unique";
    EXPECT_TRUE(foundInterests.count("MegaHub")) << "Missing interest: MegaHub";
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {});
}
