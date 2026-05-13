#pragma once

#include <memory>

#include "AbstractTCPParser.h"

#include "BioAssert.h"

namespace net {
class BaseConnectionState {
public:
    BaseConnectionState();
    virtual ~BaseConnectionState();


    // Builds the parser and writer. Each factory receives `this` so the
    // parser/writer can hold a back-reference for accessing shared state
    // (e.g. the nghttp2 session in H2ConnectionState).
    virtual void init(CreateAbstractTCPWriterFunc writerFunc,
                      CreateAbstractTCPParserFunc parserFunc,
                      NetBuffer* buffer);

    AbstractTCPWriter& getWriter() {
        return *_writer;
    }

    AbstractTCPParser& getParser() {
        return *_parser;
    }

    template <std::derived_from<AbstractTCPParser> ParserT>
    ParserT& getParser() {
        return *static_cast<ParserT*>(_parser.get());
    }

    template <std::derived_from<AbstractTCPWriter> WriterT>
    WriterT& getWriter() {
        return *static_cast<WriterT*>(_writer.get());
    }
private:
    std::unique_ptr<AbstractTCPWriter> _writer;
    std::unique_ptr<AbstractTCPParser> _parser;
};
}
