#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/Column.h"
#include "columns/ColumnMask.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Flags = std::vector<bool>;

// n:Person over the eighteen simpledb nodes in name order: Adam, Animals, Bio, Computers,
// Cooking, Cyrus, Doruk, Eighties, Ghosts, Gym, JiuJitsu, Luc, Martina, Maxime, Padel,
// Remy, Suhas, Travel
const Flags peopleByName = {
    true, false, false, false, false, true, true, false, false,
    false, false, true, true, true, false, true, true, false,
};

// Collects the one mask column a query emits, and the type name of any chunk that
// reached the sink as something other than a mask
class MaskSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* mask = dynamic_cast<const ColumnMask*>(chunks[0]);
        if (!mask) {
            _otherKinds.emplace_back(chunks[0]->getTypeName());
            return;
        }

        const std::vector<ColumnMask::Bool_t>& raw = mask->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(static_cast<bool>(raw[rowIndex]));
        }
    }

    const Flags& values() const { return _values; }
    const std::vector<std::string>& otherKinds() const { return _otherKinds; }

private:
    Flags _values;
    std::vector<std::string> _otherKinds;
};

}

// A label test is a mask, and the ops that carry a column past its producer - the sort,
// the limit, the skip, the dedup, the cross product - hand it on as the same mask. None
// of them turns it into a plain boolean column on the way.
class MaskColumnCarryTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectMask(std::string_view query, const Flags& expected) {
        MaskSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        ASSERT_TRUE(sink.otherKinds().empty())
            << "query: " << query << "\nemitted as " << sink.otherKinds().front();
        EXPECT_EQ(sink.values(), expected) << "query: " << query;
    }

    void expectMaskRowSet(std::string_view query, const Flags& expected) {
        MaskSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        ASSERT_TRUE(sink.otherKinds().empty())
            << "query: " << query << "\nemitted as " << sink.otherKinds().front();

        Flags actual = sink.values();
        std::sort(actual.begin(), actual.end());

        Flags sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MaskColumnCarryTest, emitsTheLabelTestAsAMask) {
    expectMaskRowSet("MATCH (n) RETURN n:Person", peopleByName);
}

TEST_F(MaskColumnCarryTest, sortsTheMaskByAnotherColumn) {
    expectMask("MATCH (n) RETURN n:Person ORDER BY n.name", peopleByName);
}

TEST_F(MaskColumnCarryTest, sortsTheMaskByItself) {
    expectMask("MATCH (n) RETURN n:Person ORDER BY n:Person",
               {false, false, false, false, false, false, false, false, false, false,
                true, true, true, true, true, true, true, true});
}

TEST_F(MaskColumnCarryTest, sortsTheMaskByItselfDescending) {
    expectMask("MATCH (n) RETURN n:Person ORDER BY n:Person DESC",
               {true, true, true, true, true, true, true, true,
                false, false, false, false, false, false, false, false, false, false});
}

// The limit fuses into the sort as its top-K bound, so the mask goes through the bounded
// accumulator's trim as well as the emit.
TEST_F(MaskColumnCarryTest, keepsTheFirstRowsOfTheSortedMask) {
    expectMask("MATCH (n) RETURN n:Person ORDER BY n.name LIMIT 3", {true, false, false});
}

TEST_F(MaskColumnCarryTest, skipsRowsOfTheSortedMask) {
    expectMask("MATCH (n) RETURN n:Person ORDER BY n.name SKIP 4 LIMIT 3", {false, true, true});
}

TEST_F(MaskColumnCarryTest, truncatesTheMask) {
    expectMask("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' RETURN n:Person LIMIT 2",
               {true, false});
}

TEST_F(MaskColumnCarryTest, skipsRowsOfTheMask) {
    expectMask("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' RETURN n:Person SKIP 1",
               {false});
}

TEST_F(MaskColumnCarryTest, dedupsTheMask) {
    expectMask("MATCH (n) RETURN DISTINCT n:Person ORDER BY n:Person", {false, true});
}

TEST_F(MaskColumnCarryTest, crossesTheMaskWithAnotherPattern) {
    expectMask("MATCH (n) WHERE n.name = 'Remy' WITH n:Person AS p "
               "MATCH (m) WHERE m.name = 'Ghosts' OR m.name = 'Adam' RETURN p",
               {true, true});
}

// A collect, a null test and a comparison with null read their operand as a nullable
// value, so the mask is read as one right where it is bound. Remy is node 0 and Ghosts
// node 6, which is the order the two rows come in.
TEST_F(MaskColumnCarryTest, collectsTheMask) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' RETURN collect(n:Person)",
               {{"[true, false]"}});
}

TEST_F(MaskColumnCarryTest, collectsTheDistinctMaskValues) {
    expectRows("MATCH (n) RETURN collect(DISTINCT n:Person)", {{"[true, false]"}});
}

TEST_F(MaskColumnCarryTest, collectsTheMaskPerGroup) {
    expectRows("MATCH (n) RETURN n:Founder, collect(DISTINCT n:Person)",
               {{"true", "[true]"}, {"false", "[false, true]"}});
}

TEST_F(MaskColumnCarryTest, testsTheMaskForNull) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' RETURN n.name, n:Person IS NULL",
               {{"Ghosts", "false"}, {"Remy", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
