#include "TCPConnectionManager.h"

#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include "ServerContext.h"
#include "TCPConnection.h"

using namespace net;

TCPConnectionManager::TCPConnectionManager(ServerContext& ctxt)
    : _ctxt(ctxt)
{
}

void TCPConnectionManager::process(AbstractThreadContext* threadContext,
                                   utils::EpollEvent& ev) {
    uint32_t eventType = ev.events;
    auto& connection = *static_cast<TCPConnection*>(ev.data);
    utils::Socket s = connection.getSocket();

    // Process Connection
    if (eventType & (utils::EVENT_RDHUP | utils::EVENT_HUP)) {
        connection.close();
        return;
    }

    if (eventType & utils::EVENT_IN) {
        auto inputWriter = connection.getInputBuffer().getWriter();
        const ssize_t bytesRead = ::recv(s, inputWriter.getBuffer(), inputWriter.getBufferSize(), 0);
        if (bytesRead <= 0) {
            connection.close();
            return;
        }
        inputWriter.setWrittenBytes(bytesRead);

        auto* parser = connection.getParser();
        auto analyzeRes = parser->analyze();
        auto& writer = connection.getWriter();

        if (!analyzeRes) {
            // Analyze HTTP Request failed
            switch (analyzeRes.error()) {
                case net::HTTP::Error::REQUEST_TOO_BIG: {
                    writer.setFirstLine(net::HTTP::Status::CONTENT_TOO_LARGE);
                    break;
                }
                case net::HTTP::Error::HEADER_INCOMPLETE: {
                    writer.setFirstLine(net::HTTP::Status::BAD_REQUEST);
                    break;
                }
                case net::HTTP::Error::TOO_MANY_PARAMS: {
                    writer.setFirstLine(net::HTTP::Status::CONTENT_TOO_LARGE);
                    break;
                }
                case net::HTTP::Error::UNKNOWN_ENDPOINT: {
                    writer.setFirstLine(net::HTTP::Status::NOT_FOUND);
                    break;
                }
                case net::HTTP::Error::INVALID_METHOD: {
                    writer.setFirstLine(net::HTTP::Status::METHOD_NOT_ALLOWED);
                    break;
                }
                case net::HTTP::Error::NO_METHOD:
                case net::HTTP::Error::NO_URI:
                case net::HTTP::Error::UNKNOWN:
                case net::HTTP::Error::INVALID_URI:
                case net::HTTP::Error::_SIZE: {
                    writer.setFirstLine(net::HTTP::Status::BAD_REQUEST);
                    break;
                }
            }
            writer.addConnection(net::getConnectionHeader(true));
            writer.addChunkedTransferEncoding();
            writer.addContentType(net::ContentType::JSON);
            writer.endHeader();
            writer.flushHeader();
            writer.flush();
            writer.reset();
            parser->reset();
            inputWriter.reset();
            connection.close();
            return;
        }

        const bool finished = analyzeRes.value();

        if (finished) {
            // Process with stored callback
            _ctxt._process(threadContext, connection);

            if (writer.getBytesWritten() != 0) {
                writer.flush(); // Make sure we sent everything
            }

            if (writer.errorOccured()) {
                writer.reset();
                parser->reset();
                inputWriter.reset();
                connection.close();
                return;
            }

            if (writer.wroteNonEmptyChunk()) {
                writer.flush();
                writer.reset();
            }

            // Reset for next query
            parser->reset();
            inputWriter.reset();

            if (connection.isCloseRequired()) {
                connection.close();
                return;
            }
        }
    }

    ev.events = utils::EVENT_IN | utils::EVENT_ET | utils::EVENT_ONESHOT;
    ev.data = &connection;
    if (!utils::epollMod(_ctxt._instance, s, ev)) {
        utils::logError("EpollMod existing connection");
        _ctxt.encounteredError(FlowStatus::CTL_ERROR);
    }
}
