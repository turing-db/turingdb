#include "TuringTest.h"
#include "ByteBuffer.h"
#include "ByteBufferIterator.h"

using namespace turing::test;

class ByteBufferIteratorTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

template <fs::TrivialPrimitive T>
void appendBytes(fs::ByteBuffer& buf, std::vector<T>& data) {
    const size_t prevSize = buf.size();
    const size_t size = data.size() * sizeof(T);
    buf.resize(prevSize + size);
    std::memcpy(buf.data() + prevSize, data.data(), size);
}

template <fs::TrivialPrimitive T>
void appendBytes(fs::ByteBuffer& buf, T data) {
    const size_t prevSize = buf.size();
    const size_t size = sizeof(T);
    buf.resize(prevSize + size);
    std::memcpy(buf.data() + prevSize, &data, size);
}

template <fs::CharPrimitive T>
void appendBytes(fs::ByteBuffer& buf, std::basic_string_view<T> data) {
    const size_t prevSize = buf.size();
    const size_t size = data.size() * sizeof(T);
    buf.resize(prevSize + size);
    std::memcpy(buf.data() + prevSize, data.data(), size);
}

TEST_F(ByteBufferIteratorTest, General) {
    using namespace std::literals;

    fs::ByteBuffer buf;

    // Vector inputs
    std::vector<uint16_t> integers = {0, 1, 2, 3, 4};
    std::vector<float> floats = {3.14159f, 1.61803f};

    appendBytes(buf, integers);
    appendBytes(buf, floats);
    appendBytes(buf, 2.71828);
    appendBytes(buf, "Hello world"sv);

    // Reading
    fs::ByteBufferIterator it {buf};
    std::span<const uint16_t> v1 = it.get<uint16_t>(3);
    ASSERT_EQ(v1[0], 0);
    ASSERT_EQ(v1[1], 1);
    ASSERT_EQ(v1[2], 2);
    ASSERT_EQ(it.get<uint16_t>(), 3);
    ASSERT_EQ(it.get<uint16_t>(), 4);
    ASSERT_NEAR(it.get<float>(), 3.14159f, 0.00001f);
    ASSERT_NEAR(it.get<float>(), 1.61803f, 0.00001f);
    ASSERT_NEAR(it.get<double>(), 2.71828, 0.00001);
    ASSERT_TRUE(it.get<char>(11) == "Hello world"sv);
}

