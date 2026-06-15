#pragma once

#include "TuringProtoHeaders.h"

#include <stddef.h>
#include <string>

namespace forge {

// Fully resolved benchmark configuration, assembled from the command-line arguments in
// main() and then read (never mutated) by the worker threads.
class ForgeConfig {
public:
    const std::string& getAddress() const { return _address; }
    void setAddress(const std::string& address) { _address = address; }

    const std::string& getPort() const { return _port; }
    void setPort(const std::string& port) { _port = port; }

    const std::string& getGraphName() const { return _graphName; }
    void setGraphName(const std::string& graphName) { _graphName = graphName; }

    const std::string& getQueriesPath() const { return _queriesPath; }
    void setQueriesPath(const std::string& queriesPath) { _queriesPath = queriesPath; }

    const std::string& getOutputPath() const { return _outputPath; }
    void setOutputPath(const std::string& outputPath) { _outputPath = outputPath; }

    const std::string& getAnimation() const { return _animation; }
    void setAnimation(const std::string& animation) { _animation = animation; }

    size_t getThreadCount() const { return _threadCount; }
    void setThreadCount(size_t threadCount) { _threadCount = threadCount; }

    size_t getConnectionCount() const { return _connectionCount; }
    void setConnectionCount(size_t connectionCount) { _connectionCount = connectionCount; }

    size_t getBufferCapacity() const { return _bufferCapacity; }
    void setBufferCapacity(size_t bufferCapacity) { _bufferCapacity = bufferCapacity; }

    double getDurationSeconds() const { return _durationSeconds; }
    void setDurationSeconds(double durationSeconds) { _durationSeconds = durationSeconds; }

    double getWarmupSeconds() const { return _warmupSeconds; }
    void setWarmupSeconds(double warmupSeconds) { _warmupSeconds = warmupSeconds; }

    double getTimeoutSeconds() const { return _timeoutSeconds; }
    void setTimeoutSeconds(double timeoutSeconds) { _timeoutSeconds = timeoutSeconds; }

private:
    std::string _address;
    std::string _port;
    std::string _graphName;
    std::string _queriesPath;
    std::string _outputPath;
    std::string _animation;
    size_t _threadCount {0};
    size_t _connectionCount {0};
    size_t _bufferCapacity {net::proto::DEFAULT_BUFFER_CAPACITY};
    double _durationSeconds {10.0};
    double _warmupSeconds {0.0};
    double _timeoutSeconds {0.0};
};

}
