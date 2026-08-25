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
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the lists of strings a keyless collect emits, one per row.
class StringListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listsRaw = lists->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::vector<std::string>& list = _lists.emplace_back();
            for (const ListElementView& element : listsRaw[rowIndex]) {
                list.push_back(std::string(element.getAs<std::string_view>()));
            }
        }
    }

    const std::vector<std::vector<std::string>>& getLists() const { return _lists; }

private:
    std::vector<std::vector<std::string>> _lists;
};

}

class CallYieldedCollectTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// CALL db.labels() YIELD label RETURN collect(label): the gathered column is a plain
// string column a procedure yielded, and the one list holds every label.
TEST_F(CallYieldedCollectTest, collectsAYieldedString) {
    StringListSink sink;
    runQuery("CALL db.labels() YIELD label RETURN collect(label)", sink);

    const std::vector<std::vector<std::string>>& lists = sink.getLists();
    ASSERT_EQ(lists.size(), 1u);

    std::vector<std::string> labels = lists.front();
    std::sort(labels.begin(), labels.end());

    const std::vector<std::string> expected {"Bioinformatics",
                                             "Exotic",
                                             "Founder",
                                             "Interest",
                                             "Person",
                                             "Sales",
                                             "SleepDisturber",
                                             "SoftwareEngineering",
                                             "Supernatural"};
    EXPECT_EQ(labels, expected);
}
