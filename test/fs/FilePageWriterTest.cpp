#include "FileReader.h"
#include "TuringTest.h"

#include "File.h"
#include "Path.h"
#include "FilePageWriter.h"

class FilePageWriterTest : public TuringTest {
protected:
    void initialize() override {
        _testfilePath = fs::Path {_outDir + "/testfile"};
    }

    void terminate() override {
    }

    template <std::integral IntegerT>
    void testOnes(size_t count) {
        {
            auto writer = fs::FilePageWriter::open(_testfilePath);
            ASSERT_TRUE(writer.has_value());
            std::vector<IntegerT> ones(count, (IntegerT)1);
            writer->write(std::span {ones});
        }

        auto f = fs::File::open(_testfilePath);
        ASSERT_TRUE(f);

        fs::FileReader reader;
        reader.setFile(&f.value());
        reader.read();
        ASSERT_FALSE(reader.errorOccured());

        auto it = reader.iterateBuffer();
        size_t sum = 0;

        for (; it != it.end();) {
            sum += it.get<IntegerT>();
        }

        ASSERT_EQ(sum, count);
    }

    fs::Path _testfilePath;
};

TEST_F(FilePageWriterTest, HardwareChecks) {
    static const size_t systemPageSize = sysconf(_SC_PAGE_SIZE);
    ASSERT_GT(fs::FilePageWriter::PAGE_SIZE, systemPageSize);
    ASSERT_TRUE(fs::FilePageWriter::PAGE_SIZE % systemPageSize == 0);
    ASSERT_TRUE(fs::FilePageWriter::InternalBuffer::ALIGNMENT % systemPageSize == 0);
}

TEST_F(FilePageWriterTest, Pages) {
    testOnes<uint64_t>(0);
    testOnes<uint64_t>(7);
    testOnes<uint64_t>(512);
    testOnes<uint64_t>(1024);
    testOnes<uint64_t>(1024ul * 1024); // Exactly one page (1MiB * sizeof(uint64_t))
    testOnes<uint64_t>(1024ul * 1024 + 7);
    testOnes<uint64_t>(1024ul * 1024 * 2); // Exactly two pages (2MiB * sizeof(uint64_t))
    testOnes<uint64_t>(1024ul * 1024 * 2 + 7);
}

TEST_F(FilePageWriterTest, Types) {
    testOnes<uint8_t>(0);
    testOnes<uint8_t>(7);
    testOnes<uint8_t>(512);
    testOnes<uint8_t>(1024);
    testOnes<uint8_t>(1024ul * 1024);
    testOnes<uint8_t>(1024ul * 1024 + 7);
    testOnes<uint8_t>(1024ul * 1024 * 2);
    testOnes<uint8_t>(1024ul * 1024 * 2 + 7);

    testOnes<int8_t>(7);
    testOnes<int8_t>(512);
    testOnes<int8_t>(1024);
    testOnes<int8_t>(1024ul * 1024);
    testOnes<int8_t>(1024ul * 1024 + 7);
    testOnes<int8_t>(1024ul * 1024 * 2);
    testOnes<int8_t>(1024ul * 1024 * 2 + 7);

    {
        constexpr std::string_view helloWorld = "Hello world!";
        {
            auto writer = fs::FilePageWriter::open(_testfilePath);
            ASSERT_TRUE(writer.has_value());
            writer->write(helloWorld);
        }

        {
            auto f = fs::File::open(_testfilePath);
            ASSERT_TRUE(f);

            fs::FileReader reader;
            reader.setFile(&f.value());
            reader.read();
            ASSERT_FALSE(reader.errorOccured());

            auto it = reader.iterateBuffer();

            std::string_view str = it.get<char>(helloWorld.size());
            ASSERT_TRUE(str == helloWorld);
        }
    }
}
