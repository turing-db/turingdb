#include "TCPListener.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <string_view>

#include "ServerContext.h"
#include "TCPConnectionStorage.h"

using namespace net;

inline constexpr std::string_view busyResponse = "HTTP/1.1 503 Service Unavailable\r\n"
                                                 "Context-Type: text/plain\r\n"
                                                 "Connection: close\r\n\r\n";

TCPListener::TCPListener(ServerContext& context)
    : _ctxt(context)
{
}

void TCPListener::accept(utils::EpollEvent& ev) {
    utils::DataSocket s {};

    while ((s = ::accept(_ctxt._socket, nullptr, nullptr)) > 0) {
        if (!utils::setNonBlock(s)) {
            utils::logError("SetNonBlock");
            _ctxt.encounteredError(FlowStatus::OPT_NONBLOCK_ERR);
        }

        if (!utils::setKeepAlive(s)) {
            utils::logError("SetKeepAlive");
            _ctxt.encounteredError(FlowStatus::OPT_KEEPALIVE_ERR);
        }

        if (!utils::setNoDelay(s)) {
            utils::logError("SetNoDelay");
            _ctxt.encounteredError(FlowStatus::OPT_NODELAY_ERR);
        }

        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT | EPOLLRDHUP | EPOLLHUP;
        ev.data.ptr = _ctxt._connections.alloc(s);

        if (!ev.data.ptr) {
            ::send(s, busyResponse.data(), busyResponse.size(), 0);
            ::shutdown(s, SHUT_RDWR);
            ::close(s);

            ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            ev.data.ptr = &_ctxt._serverConnection;
            if (!utils::epollMod(_ctxt._instance, _ctxt._socket, ev)) {
                utils::logError("EpollMod server accept");
                _ctxt.encounteredError(FlowStatus::CTL_ERROR);
            }

            continue;
        }

        if (!utils::epollAdd(_ctxt._instance, s, ev)) {
            utils::logError("EpollAdd new connection");
            _ctxt.encounteredError(FlowStatus::CTL_ERROR);
        }

        ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
        ev.data.ptr = &_ctxt._serverConnection;
        if (!utils::epollMod(_ctxt._instance, _ctxt._socket, ev)) {
            utils::logError("EpollMod server accept");
            _ctxt.encounteredError(FlowStatus::CTL_ERROR);
        }
    }
}
