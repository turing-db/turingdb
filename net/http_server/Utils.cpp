#include "Utils.h"

#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#ifdef __APPLE__
#include <sys/event.h>
#else
#include <sys/epoll.h>
#include <sys/signalfd.h>
#endif

#include <spdlog/spdlog.h>

namespace net::utils {

static_assert(INET_ADDRSTRLEN == ADDR_LEN);

bool setNoDelay(Socket s, bool enable) {
    int opt = enable;
    return setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) != -1;
}

bool setReuseAddress(ServerSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) != -1;
}

bool setReusePort(ServerSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) != -1;
}

bool setKeepAlive(DataSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(int)) != -1
        && setKeepAliveIdle(s, 1)
        && setKeepAliveInterval(s, 1)
        && setKeepAliveCount(s, 10);
}

bool setKeepAliveIdle(DataSocket s, int idleTime) {
#ifdef __APPLE__
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPALIVE, &idleTime, sizeof(int)) != -1;
#else
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPIDLE, &idleTime, sizeof(int)) != -1;
#endif
}

bool setKeepAliveInterval(DataSocket s, int interval) {
#ifdef __APPLE__
    (void)s; (void)interval;  // Not available on macOS
    return true;
#else
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(int)) != -1;
#endif
}

bool setKeepAliveCount(DataSocket s, int count) {
#ifdef __APPLE__
    (void)s; (void)count;  // Not available on macOS
    return true;
#else
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(int)) != -1;
#endif
}

bool setNonBlock(Socket s, bool enable) {
    int flags = fcntl(s, F_GETFL, 0);
    if (flags == -1) {
        return false;
    }

    return enable
             ? fcntl(s, F_SETFL, flags | O_NONBLOCK) != -1
             : fcntl(s, F_SETFL, flags & ~O_NONBLOCK) != -1;
}

inline constexpr socklen_t socketAddrLen = sizeof(struct sockaddr_in);

bool bind(ServerSocket s, const char* address, uint32_t port) {
    sockaddr_in socketAddr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {.s_addr = inet_addr(address)},
    };

    return bind(s, (struct sockaddr*)(&socketAddr), socketAddrLen) != -1;
}

bool listen(ServerSocket s) {
    return ::listen(s, 32) != -1;
}

bool epollAdd(EpollInstance instance, Socket fd, EpollEvent& event) {
#ifdef __APPLE__
    struct kevent kev;
    uint16_t flags = EV_ADD;
    if (event.events & EVENT_ET) flags |= EV_CLEAR;
    if (event.events & EVENT_ONESHOT) flags |= EV_ONESHOT;
    EV_SET(&kev, fd, EVFILT_READ, flags, 0, 0, event.data);
    return kevent(instance, &kev, 1, nullptr, 0, nullptr) != -1;
#else
    epoll_event ev { .events = event.events, .data = {.ptr = event.data} };
    return epoll_ctl(instance, EPOLL_CTL_ADD, fd, &ev) != -1;
#endif
}

bool epollMod(EpollInstance instance, Socket fd, EpollEvent& event) {
#ifdef __APPLE__
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    kevent(instance, &kev, 1, nullptr, 0, nullptr);
    return epollAdd(instance, fd, event);
#else
    epoll_event ev { .events = event.events, .data = {.ptr = event.data} };
    return epoll_ctl(instance, EPOLL_CTL_MOD, fd, &ev) != -1;
#endif
}

bool epollDel(EpollInstance instance, Socket fd, EpollEvent& event) {
#ifdef __APPLE__
    (void)event;
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    kevent(instance, &kev, 1, nullptr, 0, nullptr);
    return true;
#else
    epoll_event ev { .events = event.events, .data = {.ptr = event.data} };
    return epoll_ctl(instance, EPOLL_CTL_DEL, fd, &ev) != -1;
#endif
}

EpollInstance createEventInstance() {
#ifdef __APPLE__
    return kqueue();
#else
    return epoll_create1(0);
#endif
}

int eventWait(EpollInstance instance, EpollEvent* events, int maxEvents, int timeout) {
    constexpr int MAX_EVENTS = 64;
    if (maxEvents > MAX_EVENTS) maxEvents = MAX_EVENTS;

#ifdef __APPLE__
    struct kevent kevents[MAX_EVENTS];
    struct timespec* ts = nullptr;
    struct timespec timeout_ts;
    if (timeout >= 0) {
        timeout_ts.tv_sec = timeout / 1000;
        timeout_ts.tv_nsec = (timeout % 1000) * 1000000;
        ts = &timeout_ts;
    }
    int n = kevent(instance, nullptr, 0, kevents, maxEvents, ts);
    for (int i = 0; i < n; i++) {
        events[i].events = 0;
        events[i].data = kevents[i].udata;
        if (kevents[i].filter == EVFILT_READ || kevents[i].filter == EVFILT_SIGNAL)
            events[i].events |= EVENT_IN;
        if (kevents[i].flags & EV_EOF)
            events[i].events |= EVENT_RDHUP | EVENT_HUP;
    }
    return n;
#else
    // Convert our EpollEvent array to epoll_event array
    epoll_event epollEvents[MAX_EVENTS];
    int n = epoll_wait(instance, epollEvents, maxEvents, timeout);
    for (int i = 0; i < n; i++) {
        events[i].events = epollEvents[i].events;
        events[i].data = epollEvents[i].data.ptr;
    }
    return n;
#endif
}

EpollSignal createSignalFd(EpollInstance instance) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    sigprocmask(SIG_BLOCK, &mask, nullptr);
#ifdef __APPLE__
    struct kevent kev[2];
    EV_SET(&kev[0], SIGTERM, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
    EV_SET(&kev[1], SIGINT, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
    kevent(instance, kev, 2, nullptr, 0, nullptr);
    return -1;  // Sentinel: signals come through kqueue directly
#else
    (void)instance;
    return signalfd(-1, &mask, 0);
#endif
}

void logError(const char* title) {
    spdlog::error("[{}]: {}", title, strerror(errno));
}

StringAddress getStringAddress(uint32_t intAddr) {
    StringAddress addr {};
    inet_ntop(AF_INET, &intAddr, addr.data(), INET_ADDRSTRLEN);
    return addr;
}

}
