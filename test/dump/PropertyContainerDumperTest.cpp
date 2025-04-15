#include "TuringTest.h"

#include "PropertyContainerDumper.h"

using namespace db;
using namespace turing::test;

class PropertyContainerDumperTest : public TuringTest {
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(PropertyContainerDumperTest, emptyStrings) {
    fs::Path outDir {_outDir.c_str()};

    auto writer = fs::FilePageWriter::open(outDir / "strings");
    ASSERT_TRUE(writer);

    StringPropertyContainerDumper dumper {writer.value()};

    TypedPropertyContainer<types::String> container;
    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, emptyInts) {
    fs::Path outDir {_outDir.c_str()};

    auto writer = fs::FilePageWriter::open(outDir / "ints");
    ASSERT_TRUE(writer);

    TrivialPropertyContainerDumper<types::Int64> dumper {writer.value()};

    TypedPropertyContainer<types::Int64> container;
    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, manyStrings) {
    fs::Path outDir {_outDir.c_str()};

    auto writer = fs::FilePageWriter::open(outDir / "strings");
    ASSERT_TRUE(writer);

    StringPropertyContainerDumper dumper {writer.value()};

    TypedPropertyContainer<types::String> container;

    static constexpr std::string_view str = "Hello, world!";
    for (EntityID id = 0; id < 100'000; id++) {
        container.add(id, str);
    }

    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, manyInts) {
    fs::Path outDir {_outDir.c_str()};

    auto writer = fs::FilePageWriter::open(outDir / "ints");
    ASSERT_TRUE(writer);

    TrivialPropertyContainerDumper<types::Int64> dumper {writer.value()};

    TypedPropertyContainer<types::Int64> container;

    for (EntityID id = 0; id < 1000'000; id++) {
        container.add(id, (types::Int64::Primitive)id.getValue());
    }

    ASSERT_TRUE(dumper.dump(container));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
