#pragma once

#include <array>
#include <cstdint>

struct epoll_event;

namespace net::utils {

inline constexpr std::size_t ADDR_LEN = 16;

using Socket = int;
using ServerSocket = int;
using DataSocket = int;
using EpollInstance = int;
using EpollEvent = ::epoll_event;
using EpollSignal = int;
using StringAddress = std::array<char, ADDR_LEN>;
using UserData = void*;


// @brief sets TCP_NODELAY on a socket
//
// If set, disable the Nagle algorithm. This means that
// segments are always sent as soon as possible, even if
// there is only a small amount of data. When not set, data
// is buffered until there is a sufficient amount to send
// out, thereby avoiding the frequent sending of small
// packets, which results in poor utilization of the network.
// This option is overridden by TCP_CORK; however, setting
// this option forces an explicit flush of pending output,
// even if TCP_CORK is currently set.
[[nodiscard]] bool setNoDelay(Socket, bool enable = true);

/// @brief sets SO_REUSEADDR on a socket
///
// Indicates that the rules used in validating addresses
// supplied in a bind(2) call should allow reuse of local
// addresses.  For AF_INET sockets this means that a socket
// may bind, except when there is an active listening socket
// bound to the address.  When the listening socket is bound
// to INADDR_ANY with a specific port then it is not possible
// to bind to this port for any local address.  Argument is
// an integer boolean flag.
[[nodiscard]] bool setReuseAddress(ServerSocket, bool enable = true);

/// @brief sets SO_REUSEPORT on a socket
//
// Permits multiple AF_INET or AF_INET6 sockets to be bound
// to an identical socket address.  This option must be set
// on each socket (including the first socket) prior to
// calling bind(2) on the socket.  To prevent port hijacking,
// all of the processes binding to the same address must have
// the same effective UID.  This option can be employed with
// both TCP and UDP sockets.
//
// For TCP sockets, this option allows accept(2) load
// distribution in a multi-threaded server to be improved by
// using a distinct listener socket for each thread.  This
// provides improved load distribution as compared to
// traditional techniques such using a single accept(2)ing
// thread that distributes connections, or having multiple
// threads that compete to accept(2) from the same socket.
//
// For UDP sockets, the use of this option can provide better
// distribution of incoming datagrams to multiple processes
// (or threads) as compared to the traditional technique of
// having multiple processes compete to receive datagrams on
// the same socket.
[[nodiscard]] bool setReusePort(ServerSocket, bool enable = true);

/// @brief sets TCP_KEEPALIVE on a socket
//
// Also sets
// - TCP_KEEPIDLE = 1
// - TCP_KEEPINTVL = 1
// - TCP_KEEPCNT = 10
[[nodiscard]] bool setKeepAlive(DataSocket, bool enable = true);

/// @brief sets TCP_KEEPIDLE on a KeepAlive socket
//
// The time (in seconds) the connection needs to remain idle
// before TCP starts sending keepalive probes.
[[nodiscard]] bool setKeepAliveIdle(DataSocket, int idleTime);

/// @brief sets TCP_KEEPINTVL on a KeepAlive socket
//
// The time (in seconds) between individual keepalive probes.
[[nodiscard]] bool setKeepAliveInterval(DataSocket, int interval);

/// @brief sets TCP_KEEPCNT on a KeepAlive socket
//
// The maximum number of keepalive probes TCP should send
// before dropping the connection.
[[nodiscard]] bool setKeepAliveCount(DataSocket, int count);

/// @brief sets O_NONBLOCK on a socket
[[nodiscard]] bool setNonBlock(Socket, bool enable = true);

[[nodiscard]] bool bind(ServerSocket, const char* address, uint32_t port);
[[nodiscard]] bool listen(ServerSocket);
[[nodiscard]] bool epollAdd(EpollInstance, Socket fd, EpollEvent&);
[[nodiscard]] bool epollMod(EpollInstance, Socket fd, EpollEvent&);
[[nodiscard]] bool epollDel(EpollInstance, Socket fd, EpollEvent&);
[[nodiscard]] StringAddress getStringAddress(uint32_t intAddr);

void logError(const char* title);
}
