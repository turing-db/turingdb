#pragma once

#include <vector>
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace net {

class IOContextThreadPool {
public:
    using IOContext = boost::asio::io_context;
    using IOContextGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

    explicit IOContextThreadPool(size_t poolSize);
    ~IOContextThreadPool();

    void run();

    void waitForShutdown();

    void shutdown();

    IOContext& getIOContext() { return _ioContext; }

private:
    size_t _poolSize {1};
    IOContext _ioContext;
    IOContextGuard _guard; // prevents io context from returning if empty
    std::vector<std::thread> _threads;
};

}

