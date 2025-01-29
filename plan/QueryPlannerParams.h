#pragma once

#include <optional>
#include <ostream>

namespace net {
class NetWriter;
}

namespace db {

class EncodingParams {
public:
    enum class EncodingType {
        NONE,
        JSON,
        DEBUG_DUMP
    };

    EncodingParams() = default;

    EncodingParams(EncodingType type, net::NetWriter* writer)
        : _type(type),
        _writer(writer)
    {
    }

    EncodingParams(EncodingType type, std::ostream& out)
        : _type(type),
        _outStream(&out)
    {
    }

    EncodingType getType() const { return _type; }

    net::NetWriter* getWriter() const { return _writer.value(); }
    std::ostream& getStream() const { return *_outStream.value(); }

private:
    EncodingType _type {EncodingType::NONE};
    std::optional<net::NetWriter*> _writer;
    std::optional<std::ostream*> _outStream;
};

}
