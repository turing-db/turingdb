#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "writers/GraphWriter.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

// =============================================================================
// COMPLEX NULL PREDICATE TESTS WITH CUSTOM GRAPH DATA
// =============================================================================

class ComplexNullPredicatesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        createPublicationGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "publicationdb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

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

    PropertyTypeID getPropID(std::string_view propertyName) {
        auto propOpt = read().getView().metadata().propTypes().get(propertyName);
        if (!propOpt) {
            throw TuringException(
                fmt::format("Failed to get property: {}.", propertyName));
        }
        return propOpt->_id;
    }

    static void createPublicationGraph(Graph* graph) {
        GraphWriter writer {graph};
        writer.setName("publicationdb");

        // Publication 1: US publication with institution
        const auto pub1 = writer.addNode({"Publication", "Academic"});
        writer.addNodeProperty<types::String>(pub1, "type", "publication");
        writer.addNodeProperty<types::String>(pub1, "country", "United States");
        writer.addNodeProperty<types::String>(pub1, "institution", "MIT");
        writer.addNodeProperty<types::Int64>(pub1, "published_year", 2023);
        writer.addNodeProperty<types::Int64>(pub1, "cited_by_count", 150);

        // Publication 2: US publication with institution
        const auto pub2 = writer.addNode({"Publication", "Academic"});
        writer.addNodeProperty<types::String>(pub2, "type", "publication");
        writer.addNodeProperty<types::String>(pub2, "country", "United States");
        writer.addNodeProperty<types::String>(pub2, "institution", "Stanford");
        writer.addNodeProperty<types::Int64>(pub2, "published_year", 2022);
        writer.addNodeProperty<types::Int64>(pub2, "cited_by_count", 300);

        // Publication 3: US publication WITHOUT institution (institution IS NULL)
        const auto pub3 = writer.addNode({"Publication", "Academic"});
        writer.addNodeProperty<types::String>(pub3, "type", "publication");
        writer.addNodeProperty<types::String>(pub3, "country", "United States");
        // No institution property set
        writer.addNodeProperty<types::Int64>(pub3, "published_year", 2021);
        writer.addNodeProperty<types::Int64>(pub3, "cited_by_count", 50);

        // Publication 4: UK publication with institution (different country)
        const auto pub4 = writer.addNode({"Publication", "Academic"});
        writer.addNodeProperty<types::String>(pub4, "type", "publication");
        writer.addNodeProperty<types::String>(pub4, "country", "United Kingdom");
        writer.addNodeProperty<types::String>(pub4, "institution", "Oxford");
        writer.addNodeProperty<types::Int64>(pub4, "published_year", 2023);
        writer.addNodeProperty<types::Int64>(pub4, "cited_by_count", 200);

        // Publication 5: US publication WITHOUT institution
        const auto pub5 = writer.addNode({"Publication", "Academic"});
        writer.addNodeProperty<types::String>(pub5, "type", "publication");
        writer.addNodeProperty<types::String>(pub5, "country", "United States");
        // No institution property set
        writer.addNodeProperty<types::Int64>(pub5, "published_year", 2020);
        // No cited_by_count property set

        // Article 1: Different type, US, with institution
        const auto article1 = writer.addNode({"Article"});
        writer.addNodeProperty<types::String>(article1, "type", "article");
        writer.addNodeProperty<types::String>(article1, "country", "United States");
        writer.addNodeProperty<types::String>(article1, "institution", "Harvard");
        writer.addNodeProperty<types::Int64>(article1, "published_year", 2023);

        // Author nodes
        const auto author1 = writer.addNode({"Author"});
        writer.addNodeProperty<types::String>(author1, "name", "John Doe");
        writer.addNodeProperty<types::String>(author1, "country", "United States");

        const auto author2 = writer.addNode({"Author"});
        writer.addNodeProperty<types::String>(author2, "name", "Jane Smith");
        writer.addNodeProperty<types::String>(author2, "country", "United Kingdom");
        writer.addNodeProperty<types::String>(author2, "institution", "Cambridge");

        // Author 3: No institution
        const auto author3 = writer.addNode({"Author"});
        writer.addNodeProperty<types::String>(author3, "name", "Bob Wilson");
        writer.addNodeProperty<types::String>(author3, "country", "United States");
        // No institution

        // Edges: AUTHORED_BY
        const auto auth1 = writer.addEdge("AUTHORED_BY", pub1, author1);
        writer.addEdgeProperty<types::String>(auth1, "role", "primary");

        const auto auth2 = writer.addEdge("AUTHORED_BY", pub2, author2);
        writer.addEdgeProperty<types::String>(auth2, "role", "primary");

        const auto auth3 = writer.addEdge("AUTHORED_BY", pub4, author2);
        writer.addEdgeProperty<types::String>(auth3, "role", "primary");

        writer.commit();
        writer.submit();
    }
};

// Test matching the Jira example: complex query with multiple string equality AND IS NOT NULL
TEST_F(ComplexNullPredicatesTest, complexMatchWithIsNotNull) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (pub)
        WHERE pub.type = 'publication'
        AND pub.country = 'United States'
        AND pub.institution IS NOT NULL
        RETURN pub.institution, pub.published_year, pub.cited_by_count
    )";

    using Int = types::Int64::Primitive;
    using String = types::String::Primitive;
    using Rows = LineContainer<String, Int, Int>;

    const PropertyTypeID typeID = getPropID("type");
    const PropertyTypeID countryID = getPropID("country");
    const PropertyTypeID institutionID = getPropID("institution");
    const PropertyTypeID publishedYearID = getPropID("published_year");
    const PropertyTypeID citedByCountID = getPropID("cited_by_count");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* type = read().tryGetNodeProperty<types::String>(typeID, n);
            const auto* country = read().tryGetNodeProperty<types::String>(countryID, n);
            const auto* institution = read().tryGetNodeProperty<types::String>(institutionID, n);
            const auto* publishedYear = read().tryGetNodeProperty<types::Int64>(publishedYearID, n);
            const auto* citedByCount = read().tryGetNodeProperty<types::Int64>(citedByCountID, n);

            if (type && *type == "publication" &&
                country && *country == "United States" &&
                institution && publishedYear && citedByCount) {
                expected.add({*institution, *publishedYear, *citedByCount});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* institutions = findColumn(df, "pub.institution")->as<ColumnOptVector<String>>();
            auto* years = findColumn(df, "pub.published_year")->as<ColumnOptVector<Int>>();
            auto* citations = findColumn(df, "pub.cited_by_count")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(institutions && years && citations);
            for (size_t row = 0; row < institutions->size(); row++) {
                actual.add({*institutions->at(row), *years->at(row), *citations->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test IS NULL with multiple string equality predicates
TEST_F(ComplexNullPredicatesTest, complexMatchWithIsNull) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (pub)
        WHERE pub.type = 'publication'
        AND pub.country = 'United States'
        AND pub.institution IS NULL
        RETURN pub, pub.published_year
    )";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<NodeID, Int>;

    const PropertyTypeID typeID = getPropID("type");
    const PropertyTypeID countryID = getPropID("country");
    const PropertyTypeID institutionID = getPropID("institution");
    const PropertyTypeID publishedYearID = getPropID("published_year");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* type = read().tryGetNodeProperty<types::String>(typeID, n);
            const auto* country = read().tryGetNodeProperty<types::String>(countryID, n);
            const auto* institution = read().tryGetNodeProperty<types::String>(institutionID, n);
            const auto* publishedYear = read().tryGetNodeProperty<types::Int64>(publishedYearID, n);

            if (type && *type == "publication" &&
                country && *country == "United States" &&
                !institution && publishedYear) {
                expected.add({n, *publishedYear});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "pub")->as<ColumnNodeIDs>();
            auto* years = findColumn(df, "pub.published_year")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(ns && years);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *years->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test combining IS NOT NULL with IS NULL on different properties
TEST_F(ComplexNullPredicatesTest, mixedNullPredicates) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (pub)
        WHERE pub.type = 'publication'
        AND pub.country = 'United States'
        AND pub.institution IS NULL
        AND pub.published_year IS NOT NULL
        RETURN pub.published_year
    )";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<Int>;

    const PropertyTypeID typeID = getPropID("type");
    const PropertyTypeID countryID = getPropID("country");
    const PropertyTypeID institutionID = getPropID("institution");
    const PropertyTypeID publishedYearID = getPropID("published_year");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* type = read().tryGetNodeProperty<types::String>(typeID, n);
            const auto* country = read().tryGetNodeProperty<types::String>(countryID, n);
            const auto* institution = read().tryGetNodeProperty<types::String>(institutionID, n);
            const auto* publishedYear = read().tryGetNodeProperty<types::Int64>(publishedYearID, n);

            if (type && *type == "publication" &&
                country && *country == "United States" &&
                !institution && publishedYear) {
                expected.add({*publishedYear});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* years = findColumn(df, "pub.published_year")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(years);
            for (size_t row = 0; row < years->size(); row++) {
                actual.add({*years->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test IS NOT NULL with OR logic
TEST_F(ComplexNullPredicatesTest, isNotNullWithOrLogic) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (n)
        WHERE (n.type = 'publication' OR n.type = 'article')
        AND n.country = 'United States'
        AND n.institution IS NOT NULL
        RETURN n, n.type, n.institution
    )";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String, String>;

    const PropertyTypeID typeID = getPropID("type");
    const PropertyTypeID countryID = getPropID("country");
    const PropertyTypeID institutionID = getPropID("institution");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* type = read().tryGetNodeProperty<types::String>(typeID, n);
            const auto* country = read().tryGetNodeProperty<types::String>(countryID, n);
            const auto* institution = read().tryGetNodeProperty<types::String>(institutionID, n);

            if (type && (*type == "publication" || *type == "article") &&
                country && *country == "United States" &&
                institution) {
                expected.add({n, *type, *institution});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* types = findColumn(df, "n.type")->as<ColumnOptVector<String>>();
            auto* institutions = findColumn(df, "n.institution")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && types && institutions);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *types->at(row), *institutions->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// Test empty result when all matching nodes have NULL for a required property
TEST_F(ComplexNullPredicatesTest, noResultWhenAllHaveNull) {
    constexpr std::string_view MATCH_QUERY = R"(
        MATCH (a)
        WHERE a.name = 'John Doe'
        AND a.institution IS NOT NULL
        RETURN a
    )";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID institutionID = getPropID("institution");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            const auto* institution = read().tryGetNodeProperty<types::String>(institutionID, n);

            if (name && *name == "John Doe" && institution) {
                expected.add({n});
            }
        }
    }
    ASSERT_EQ(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "a")->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
