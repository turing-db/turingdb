#include "FileReader.h"
#include "TuringTest.h"

#include "File.h"
#include "Path.h"
#include "FilePageWriter.h"

using namespace turing::test;

class FilePageWriterTest : public TuringTest {
protected:
    void initialize() override {
        _testfilePath = fs::Path {_outDir + "/testfile"};
    }

    void terminate() override {
    }


    template <std::integral IntegerT>
    void testOnes(size_t byteCount) {
        const size_t count = byteCount / sizeof(IntegerT);
        {
            auto writer = fs::FilePageWriter::open(_testfilePath);
            ASSERT_TRUE(writer.has_value());
            if constexpr (std::same_as<IntegerT, uint64_t>) {
                _uint64s.resize(count, (IntegerT)1);
                writer->write(std::span {_uint64s});
            } else if constexpr (std::same_as<IntegerT, uint8_t>) {
                _uint8s.resize(count, (IntegerT)1);
                writer->write(std::span {_uint8s});
            } else if constexpr (std::same_as<IntegerT, int8_t>) {
                _int8s.resize(count, (IntegerT)1);
                writer->write(std::span {_int8s});
            }
        }

        auto f = fs::File::open(_testfilePath);
        ASSERT_TRUE(f);

        _reader.setFile(&f.value());
        _reader.read();
        ASSERT_FALSE(_reader.errorOccured());

        auto it = _reader.iterateBuffer();
        size_t sum = 0;

        for (; it != it.end();) {
            sum += it.get<IntegerT>();
        }

        ASSERT_EQ(sum, count);
    }

    std::vector<uint64_t> _uint64s;
    std::vector<uint8_t> _uint8s;
    std::vector<int8_t> _int8s;
    fs::FileReader _reader;
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
    testOnes<uint64_t>(13);
    testOnes<uint64_t>(1024);
    testOnes<uint64_t>(fs::FilePageWriter::PAGE_SIZE - 1);
    testOnes<uint64_t>(fs::FilePageWriter::PAGE_SIZE);
    testOnes<uint64_t>(fs::FilePageWriter::PAGE_SIZE + 13);
    testOnes<uint64_t>(fs::FilePageWriter::PAGE_SIZE * 2);
    testOnes<uint64_t>(fs::FilePageWriter::PAGE_SIZE * 2 + 13);
}

TEST_F(FilePageWriterTest, Types) {
    testOnes<uint8_t>(0);
    testOnes<uint8_t>(13);
    testOnes<uint8_t>(1024);
    testOnes<uint8_t>(fs::FilePageWriter::PAGE_SIZE - 1);
    testOnes<uint8_t>(fs::FilePageWriter::PAGE_SIZE);
    testOnes<uint8_t>(fs::FilePageWriter::PAGE_SIZE + 13);
    testOnes<uint8_t>(fs::FilePageWriter::PAGE_SIZE * 2);
    testOnes<uint8_t>(fs::FilePageWriter::PAGE_SIZE * 2 + 13);

    testOnes<int8_t>(0);
    testOnes<int8_t>(13);
    testOnes<int8_t>(1024);
    testOnes<int8_t>(fs::FilePageWriter::PAGE_SIZE - 1);
    testOnes<int8_t>(fs::FilePageWriter::PAGE_SIZE);
    testOnes<int8_t>(fs::FilePageWriter::PAGE_SIZE + 13);
    testOnes<int8_t>(fs::FilePageWriter::PAGE_SIZE * 2);
    testOnes<int8_t>(fs::FilePageWriter::PAGE_SIZE * 2 + 13);

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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 2;
    });
}
