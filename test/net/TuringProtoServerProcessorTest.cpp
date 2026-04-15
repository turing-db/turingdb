#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define private public
#include "TuringProtoServerProcessor.h"
#include "TuringProtoWriter.h"
#undef private

#include "ProtocolException.h"
#include "TCPConnection.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoParser.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace turing::test;

namespace {

// Use a local socketpair so the tests exercise real read/write behavior
// without needing a TCP listener or background accept loop.
class SocketPair {
public:
    SocketPair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, _fds.data()) != 0) {
            throw std::runtime_error("socketpair failed");
        }
    }

    ~SocketPair() {
        for (int& fd : _fds) {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }
    }

    int sender() const { return _fds[0]; }
    int receiver() const { return _fds[1]; }

private:
    std::array<int, 2> _fds {-1, -1};
};

std::string framePacket(net::proto::MessageTypes type, std::string_view payload) {
    net::proto::TuringProtoOutBuf outBuf(
        net::proto::ProtoHeader::wireSize() + payload.size());
    outBuf.setOnBufferFullCallBack([]() {});
    net::proto::frameMessage(type, payload, &outBuf);
    return std::string(outBuf.data(), outBuf.size());
}

void attachProtocolObjects(net::TCPConnection* connection, int socketFd) {
    connection->setParser(std::make_unique<net::proto::TuringProtoParser>(&connection->getInputBuffer()));
    connection->setWriter(std::make_unique<net::proto::TuringProtoWriter>());
    connection->setSocket(socketFd);
}

// Seed a fully analyzed frame so tests can call individual handler methods
// directly instead of driving the entire server read loop.
void primeParser(net::TCPConnection* connection,
                 net::proto::MessageTypes type,
                 std::string_view payload) {
    const auto packet = framePacket(type, payload);
    auto writer = connection->getInputBuffer().getWriter();
    writer.reset();
    writer.writeString(packet.data(), packet.size());

    auto& parser = connection->getParser<net::proto::TuringProtoParser>();
    const auto result = parser.analyze();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result.value());
}

std::string receiveExact(int fd, size_t len) {
    std::string data(len, '\0');
    size_t offset = 0;

    while (offset < len) {
        const ssize_t bytesRead = ::recv(fd, data.data() + offset, len - offset, 0);
        if (bytesRead <= 0) {
            throw std::runtime_error("recv failed");
        }
        offset += static_cast<size_t>(bytesRead);
    }

    return data;
}

// Callers can intentionally lie about the embedded lengths to build malformed
// payloads for getTransactionInfo negative cases.
std::string makeQueryPayload(uint32_t graphNameLen,
                             uint32_t queryLen,
                             std::string_view graphName,
                             std::string_view query,
                             std::string_view trailing = {}) {
    net::proto::QueryWireHeader header {
        ._commitHash = 17,
        ._changeID = 23,
        ._graphNameLen = graphNameLen,
        ._queryLen = queryLen};

    std::string payload(
        net::proto::QueryWireHeader::wireSize() + graphName.size() + query.size() + trailing.size(),
        '\0');
    size_t offset = 0;
    header.copyToBuffer(payload.data(), offset);
    std::memcpy(payload.data() + offset, graphName.data(), graphName.size());
    offset += graphName.size();
    std::memcpy(payload.data() + offset, query.data(), query.size());
    offset += query.size();
    std::memcpy(payload.data() + offset, trailing.data(), trailing.size());
    return payload;
}

} // namespace

class TuringProtoServerProcessorTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _db = &_env->getDB();
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
};

// TuringProtoWriter::flush() drives writev across two iovecs that together
// exceed the kernel send buffer. The test shrinks SO_SNDBUF and the reader
// thread sleeps briefly between recv() calls so the kernel cannot accept
// the whole payload in one go: writev returns short and flush() must
// resume from the right offset (advancing iov_base / iov_len in place)
// across multiple syscalls. After flush, the reader must observe the full
// concatenated payload, no error flag should be set, and the writer's
// bytes-pending counter must be zero.
TEST(TuringProtoWriterSocketTest, FlushesLargeIovecPayloadAcrossSocketPair) {
    SocketPair sockets;
    // Shrink the kernel send buffer so flush() has to handle partial writes
    // across multiple iovecs instead of succeeding in a single syscall.
    int sendBufferSize = 512;
    ASSERT_EQ(::setsockopt(sockets.sender(), SOL_SOCKET, SO_SNDBUF,
                           &sendBufferSize, sizeof(sendBufferSize)),
              0);

    net::proto::TuringProtoWriter writer;
    writer.setSocket(sockets.sender());

    const std::string first(32 * 1024, 'a');
    const std::string second(48 * 1024, 'b');
    const std::string expected = first + second;

    std::string received;
    received.reserve(expected.size());
    std::atomic<int> readError {0};

    std::thread reader([&]() {
        std::array<char, 257> buffer {};
        while (received.size() < expected.size()) {
            const ssize_t bytesRead = ::recv(sockets.receiver(), buffer.data(), buffer.size(), 0);
            if (bytesRead <= 0) {
                readError.store(bytesRead == 0 ? -1 : errno);
                return;
            }
            received.append(buffer.data(), static_cast<size_t>(bytesRead));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    writer._iovecs[0] = {const_cast<char*>(first.data()), first.size()};
    writer._iovecs[1] = {const_cast<char*>(second.data()), second.size()};
    writer.flush();
    ::shutdown(sockets.sender(), SHUT_WR);
    reader.join();

    EXPECT_FALSE(writer.errorOccured());
    EXPECT_EQ(readError.load(), 0);
    EXPECT_EQ(received, expected);
    EXPECT_EQ(writer.getBytesWritten(), 0u);
}

// The HELLO (NABER) handshake carries a fixed-size version triple as its
// payload. A NABER frame whose payload is the wrong length must be
// rejected with ProtocolException so we never read uninitialized version
// fields or drift into the rest of the protocol with a bad client.
TEST_F(TuringProtoServerProcessorTest, HandleHelloRejectsInvalidPayloadSize) {
    SocketPair sockets;
    net::TCPConnection connection;
    attachProtocolObjects(&connection, sockets.sender());
    primeParser(&connection, net::proto::MessageTypes::NABER, std::string(2, '\0'));

    TuringProtoServerProcessor processor(*_db, connection);
    EXPECT_THROW(processor.handleHello(), ProtocolException);
}

// Happy-path HELLO: a well-formed NABER (3-byte version) must be answered
// with an IYI packet whose payload is a single boolean acknowledging
// version compatibility. Reads the response off a real socket pair so the
// header layout and payload byte are validated end-to-end as the client
// would see them.
TEST_F(TuringProtoServerProcessorTest, HandleHelloWritesAck) {
    SocketPair sockets;
    net::TCPConnection connection;
    attachProtocolObjects(&connection, sockets.sender());

    std::string payload(3, '\0');
    payload[0] = 1;
    payload[1] = 1;
    payload[2] = 0;
    primeParser(&connection, net::proto::MessageTypes::NABER, payload);

    TuringProtoServerProcessor processor(*_db, connection);
    ASSERT_NO_THROW(processor.handleHello());

    const auto response = receiveExact(sockets.receiver(), net::proto::ProtoHeader::wireSize() + sizeof(bool));
    const auto header = net::proto::ProtoHeader::decode(response.data(), response.size());
    EXPECT_EQ(header._type, net::proto::MessageTypes::IYI);
    EXPECT_EQ(header._dataLen, sizeof(bool));

    bool ack = false;
    std::memcpy(&ack, response.data() + net::proto::ProtoHeader::wireSize(), sizeof(ack));
    EXPECT_TRUE(ack);
}

// getTransactionInfo parses a QueryWireHeader at the start of the QUERY
// payload. If the payload is shorter than wireSize() the embedded length
// fields would be read out of bounds, so this case must throw before any
// memcpy of the header.
TEST_F(TuringProtoServerProcessorTest, GetTransactionInfoRejectsTruncatedHeader) {
    net::TCPConnection connection;
    TuringProtoServerProcessor processor(*_db, connection);

    const std::string payload(net::proto::QueryWireHeader::wireSize() - 1, '\0');
    EXPECT_THROW([&]() { (void)processor.getTransactionInfo(payload); }(), TuringException);
}

// The graph-name length declared in the QueryWireHeader must point inside
// the payload. A header that claims more graph-name bytes than remain
// after the wire header (here: claims 5 bytes of name in a payload that
// only has 2 bytes left) must be rejected, otherwise the server would
// build a string_view over adjacent memory.
TEST_F(TuringProtoServerProcessorTest, GetTransactionInfoRejectsGraphNameThatRunsPastPayload) {
    net::TCPConnection connection;
    TuringProtoServerProcessor processor(*_db, connection);

    const auto payload = makeQueryPayload(5, 0, "ab", "");
    EXPECT_THROW([&]() { (void)processor.getTransactionInfo(payload); }(), TuringException);
}

// Same boundary check, applied to the query string field. The graph-name
// portion of the payload is well-formed, but the declared query length
// (10) overruns the 3 query bytes actually present, so the call must
// throw rather than read past the payload.
TEST_F(TuringProtoServerProcessorTest, GetTransactionInfoRejectsQueryThatRunsPastPayload) {
    net::TCPConnection connection;
    TuringProtoServerProcessor processor(*_db, connection);

    const auto payload = makeQueryPayload(2, 10, "db", "abc");
    EXPECT_THROW([&]() { (void)processor.getTransactionInfo(payload); }(), TuringException);
}

// All declared lengths in the QueryWireHeader are honored, but the
// payload contains an extra byte after the query. The format does not
// allow trailing data, so accepting it would make the wire format
// ambiguous and could mask client bugs. The call must throw.
TEST_F(TuringProtoServerProcessorTest, GetTransactionInfoRejectsTrailingBytes) {
    net::TCPConnection connection;
    TuringProtoServerProcessor processor(*_db, connection);

    const auto payload = makeQueryPayload(2, 3, "db", "abc", "x");
    EXPECT_THROW([&]() { (void)processor.getTransactionInfo(payload); }(), TuringException);
}
