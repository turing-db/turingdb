#include "TuringProtoParser.h"

#include <utility>

#include "TuringProtoWriter.h"

using namespace net;
using namespace net::proto;

TuringProtoParser::TuringProtoParser(NetBuffer* inputBuffer)
    : _reader(inputBuffer->getReader())
{
}

AbstractTCPParser::AnalyzeResult TuringProtoParser::analyze() {
    const size_t bytesAvailable = _reader.getSize();

    if (bytesAvailable < ProtoHeader::wireSize()) {
        return false;
    }

    if (!_parsedHeader) {
        _header = ProtoHeader::decode(_reader.getData(), bytesAvailable);

        if (!ProtoHeader::isValidMessageType(_header._type)) {
            return BadResult(std::to_underlying(Error::INVALID_MESSAGE_TYPE));
        }

        const size_t totalMessageSize = ProtoHeader::wireSize() + static_cast<size_t>(_header._dataLen);
        if (totalMessageSize > NetBuffer::BUFFER_SIZE) {
            return BadResult(std::to_underlying(Error::REQUEST_TOO_BIG));
        }

        _parsedHeader = true;
    }

    const size_t totalMessageSize = ProtoHeader::wireSize() + static_cast<size_t>(_header._dataLen);
    if (bytesAvailable < totalMessageSize) {
        return false;
    }

    _payload = std::string_view(_reader.getData() + ProtoHeader::wireSize(), _header._dataLen);
    return true;
}

void TuringProtoParser::handleAnalyzeError(AnalyzeError error, AbstractTCPWriter& writer) {
    auto& protoWriter = static_cast<TuringProtoWriter&>(writer);

    switch (static_cast<Error>(error)) {
        case Error::REQUEST_TOO_BIG:
            protoWriter.writeProtocolError("Protocol packet exceeds max buffer size");
            return;
        break;

        case Error::INVALID_MESSAGE_TYPE:
            protoWriter.writeProtocolError("Invalid protocol message type");
            return;
        break;

        case Error::INVALID_HEADER:
            protoWriter.writeProtocolError("Invalid protocol header");
            return;
        break;
    }
}

void TuringProtoParser::reset() {
    _header = {};
    _payload = {};
    _parsedHeader = false;
}
