#include "TuringTest.h"

#include <vector>

#include "dump/DumpConfig.h"
#include "dump/PropertyContainerDumper.h"
#include "dump/PropertyContainerLoader.h"
#include "comparators/PropertyContainerComparator.h"
#include "list/ListContainer.h"
#include "map/MapContainer.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/PropertyNull.h"
#include "FileHash.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"

using namespace db;
using namespace turing::test;

namespace {

using ListItem = ListContainer::ListItemVariant;
using MapEntry = MapContainer::MapKeyValuePair;

constexpr float EMBEDDING_VALUES[] = {0.5f, -1.5f};

}

class CellTagDumpFormatTest : public TuringTest {
};

TEST_F(CellTagDumpFormatTest, listHoldingEveryCellTag) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "lists";

    ListContainer lists;
    const std::vector<ListItem> innerItems = {static_cast<int64_t>(1)};
    const ListView innerList = lists.insert(innerItems);

    const std::vector<MapEntry> innerEntries = {{"k", static_cast<int64_t>(2)}};
    const MapView innerMap = lists.getMaps().insert(innerEntries);

    const std::vector<ListItem> items = {static_cast<int64_t>(-3),
                                         static_cast<uint64_t>(4),
                                         5.25,
                                         types::Bool::Primitive {true},
                                         types::String::Primitive {"text"},
                                         types::Embedding::Primitive {EMBEDDING_VALUES},
                                         innerList,
                                         PropertyNull {},
                                         NodeID(6),
                                         EdgeID(7),
                                         DateTime(8),
                                         innerMap,
                                         Duration(9)};

    TypedPropertyContainer<types::List> original;
    original.add(EntityID(0), lists.insert(items));

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        ListPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    uint64_t hash = 0;
    hashDumpFileContent(path, hash);
    EXPECT_EQ(hash, 18321698996521711460ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        ListPropertyContainerLoader loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto& loaded = result.value()->cast<types::List>();
        ASSERT_TRUE(PropertyContainerComparator::same(&original, &loaded));
    }
}

TEST_F(CellTagDumpFormatTest, mapHoldingEveryCellTag) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "maps";

    MapContainer maps;
    const std::vector<ListItem> innerItems = {static_cast<int64_t>(1)};
    const ListView innerList = maps.getLists().insert(innerItems);

    const std::vector<MapEntry> innerEntries = {{"k", static_cast<int64_t>(2)}};
    const MapView innerMap = maps.insert(innerEntries);

    const std::vector<MapEntry> entries = {{"a", static_cast<int64_t>(-3)},
                                           {"b", static_cast<uint64_t>(4)},
                                           {"c", 5.25},
                                           {"d", types::Bool::Primitive {true}},
                                           {"e", types::String::Primitive {"text"}},
                                           {"f", types::Embedding::Primitive {EMBEDDING_VALUES}},
                                           {"g", innerList},
                                           {"h", PropertyNull {}},
                                           {"i", NodeID(6)},
                                           {"j", EdgeID(7)},
                                           {"k", DateTime(8)},
                                           {"l", innerMap},
                                           {"m", Duration(9)}};

    TypedPropertyContainer<types::Map> original;
    original.add(EntityID(0), maps.insert(entries));

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        MapPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    uint64_t hash = 0;
    hashDumpFileContent(path, hash);
    EXPECT_EQ(hash, 234760376920242867ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        MapPropertyContainerLoader loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto& loaded = result.value()->cast<types::Map>();
        ASSERT_TRUE(PropertyContainerComparator::same(&original, &loaded));
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
