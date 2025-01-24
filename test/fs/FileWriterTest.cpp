#include "TuringTest.h"
#include "FileWriter.h"
#include "FileReader.h"

using namespace turing::test;

class FileWriterTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(FileWriterTest, General) {
    fs::FileWriter<64> writer;

    {
        const fs::Path path {"testFile"};
        auto fileRes = fs::File::createAndOpen(path);
        ASSERT_TRUE(fileRes.has_value());

        auto file = std::move(fileRes.value());
        ASSERT_TRUE(file.clearContent());

        // FileWriter with small buffer
        writer.setFile(&file);

        std::array<uint8_t, 256> bytes {};
        bytes[0] = 0;
        bytes[3] = 3;
        bytes[6] = 6;

        writer.write(std::span {bytes});
        ASSERT_FALSE(writer.errorOccured());
        ASSERT_EQ(file.getInfo()._size, 256);
    }
}

