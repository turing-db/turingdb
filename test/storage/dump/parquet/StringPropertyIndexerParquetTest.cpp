#include "TuringTest.h"

#include "dump/parquet/StringPropertyIndexerParquetDumper.h"
#include "dump/parquet/StringPropertyIndexerParquetLoader.h"
#include "comparators/StringIndexerComparator.h"
#include "indexers/StringPropertyIndexer.h"
#include "indexes/StringIndex.h"

#include <memory>

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class StringPropertyIndexerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(StringPropertyIndexerParquetTest, RoundTrip) {
    // insert() expects preprocessed (lower-case alphanumeric) tokens.
    auto index0 = std::make_unique<StringIndex>();
    index0->insert("alice", EntityID {1});
    index0->insert("alpha", EntityID {2});
    index0->insert("bob", EntityID {3});
    index0->insert("alice", EntityID {4});

    auto index1 = std::make_unique<StringIndex>();
    index1->insert("xyz", EntityID {10});

    StringPropertyIndexer original;
    original.addIndex(PropertyTypeID {0}, std::move(index0));
    original.addIndex(PropertyTypeID {1}, std::move(index1));
    original.setInitialised();

    const fs::Path base = fs::Path(_outDir);
    const fs::Path indexesPath = base / "string-index-indexes.parquet";
    const fs::Path childrenPath = base / "string-index-children.parquet";
    const fs::Path ownersPath = base / "string-index-owners.parquet";

    StringPropertyIndexerParquetDumper::dump(original, indexesPath, childrenPath, ownersPath);

    const std::unique_ptr<StringPropertyIndexer> loaded =
        StringPropertyIndexerParquetLoader::load(indexesPath, childrenPath, ownersPath);

    EXPECT_TRUE(StringIndexerComparator::same(original, *loaded));
    EXPECT_EQ(loaded->size(), original.size());
    EXPECT_TRUE(loaded->isInitialised());
}
