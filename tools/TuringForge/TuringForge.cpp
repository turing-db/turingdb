#include <ranges>
#include <stdlib.h>
#include <stdint.h>

#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <latch>


#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>

#include "ForgeConfig.h"
#include "ForgeWorkerThread.h"
#include "ForgeConnection.h"
#include "HdrHistogram.h"
#include "ForgeErrorCounter.h"
#include "TuringAsyncClient.h"
#include "TuringProtoHeaders.h"
#include "SocketUtils.h"
#include "ForgeSpinners.h"
#include "ForgeWorkload.h"
#include "spdlog/spdlog.h"

using namespace forge;

namespace {
// Split a "host:port" string into its two parts on the last colon, so IPv6
// literals (which contain colons) keep their address intact.
bool parseServerAddress(const std::string& server, std::string& outAddress, std::string& outPort) {
    const size_t colon = server.find_last_of(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= server.size()) {
        return false;
    }

    outAddress = server.substr(0, colon);
    outPort = server.substr(colon + 1);
    return true;
}

void mergeAndPrintStats(const bool isWriteLoad,
                        const size_t numQueries,
                        const size_t connectionCount,
                        const double elapsedSeconds,
                        const std::string& testLoadLabel,
                        std::vector<std::unique_ptr<ForgeWorkerThread>>& threadStructs) {

    auto& firstThread = threadStructs.front();
    auto* const mergedWallClockHistogram = firstThread->getWallClockHistogram();
    auto* const mergedEngineTimeHistogram = firstThread->getEngineHistogram();
    auto* const mergedChangeCycleHistogram = firstThread->getChangeCycleHistogram();
    auto* const mergedErrors = firstThread->getErrorCounter();

    for (const auto& thread : threadStructs | std::ranges::views::drop(1)) {
        mergedWallClockHistogram->add(*thread->getWallClockHistogram());
        mergedEngineTimeHistogram->add(*thread->getEngineHistogram());
        mergedChangeCycleHistogram->add(*thread->getChangeCycleHistogram());
        mergedErrors->add(*thread->getErrorCounter());
    }

    const uint64_t completedLoads = mergedWallClockHistogram->getTotalCount();
    const double loadsPerSecond = elapsedSeconds > 0.0
                                    ? static_cast<double>(completedLoads) / elapsedSeconds
                                    : 0.0;
    const double queriesPerSecond = loadsPerSecond * static_cast<double>(numQueries);

    fmt::println("");
    fmt::println("== {} ==", testLoadLabel);
    fmt::println("throughput      {:.1f} loads/s  ({:.0f} queries/s)",
                 loadsPerSecond,
                 queriesPerSecond);
    fmt::println("completed       {} loads in {:.3f} s", completedLoads, elapsedSeconds);

    const uint64_t droppedConnections = mergedErrors->getDroppedConnections();
    const uint64_t activeConnections = droppedConnections < connectionCount
                                         ? connectionCount - droppedConnections
                                         : 0;
    fmt::println("connections     {} started, {} dropped, {} active at end",
                 connectionCount,
                 droppedConnections,
                 activeConnections);

    mergedWallClockHistogram->printStats(std::cout, "latency");
    mergedEngineTimeHistogram->printStats(std::cout, "engine");
    if (isWriteLoad) {
        mergedChangeCycleHistogram->printStats(std::cout, "change-cycle");
    }
    mergedErrors->printStats(std::cout);
}


void runClientThread(std::stop_token stop,
                     ForgeWorkerThread* threadStruct,
                     std::latch* connectedLatch,
                     std::latch* readyLatch,
                     TimePoint* deadline,
                     const std::vector<std::string>& queries,
                     size_t connectionCount,
                     bool isWriteLoad) {
    // Thread Startup
    net::utils::EventInstance epollInstance;
    const net::utils::EpollInstance epoll = epollInstance.getInstance();
    std::vector<std::unique_ptr<ForgeConnection>> connections;
    connections.reserve(connectionCount);

    bool allConnected = true;
    for (size_t i = 0; i < connectionCount; ++i) {
        auto& connection = connections.emplace_back(std::make_unique<ForgeConnection>(threadStruct,
                                                                                      queries,
                                                                                      isWriteLoad));

        auto* client = connection->getClient();

        try {
            client->connect();
        } catch (const std::exception& e) {
            spdlog::error("Worker failed to connect: {}", e.what());
            allConnected = false;
            break;
        }

        if (!client->isConnected()) {
            spdlog::error("Worker failed to connect");
            allConnected = false;
            break;
        }

        // Arm epoll events
        net::utils::EpollEvent ev;
        ev._events = net::utils::EVENT_ET
                   | net::utils::EVENT_IN
                   | net::utils::EVENT_OUT
                   | net::utils::EVENT_ONESHOT
                   | net::utils::EVENT_RDHUP
                   | net::utils::EVENT_HUP;

        ev._data = connection.get();
        const bool added = net::utils::epollAdd(epoll, client->getSocket(), ev);
        if (!added) {
            spdlog::error("failed to add epoll event");
            allConnected = false;
            break;
        }
    }

    if (allConnected) {
        threadStruct->setConnected();
    }

    connectedLatch->count_down();

    if (!allConnected) {
        return;
    }

    // Wait for the synchronized start, then issue queries until the deadline or
    // a stop request.
    readyLatch->wait();
    // If other threads fail to connect, the main thread will issue a stop signal
    if (stop.stop_requested()) {
        return;
    }

    ForgeErrorCounter& errors = *threadStruct->getErrorCounter();
    std::vector<net::utils::EpollEvent> events(connectionCount);

    // Kick off the first cycle on each connection
    for (auto& connection : connections) {
        try {
            connection->beginCycle();
        } catch (const std::exception&) {
            errors.recordConnectionError();
        }
    }

    // Main event loop
    while (!stop.stop_requested()) {
        if (Clock::now() >= *deadline) {
            break;
        }

        const int nfds = net::utils::eventWait(epoll, events.data(), static_cast<int>(events.size()), 1000);
        if (nfds <= 0) {
            continue;
        }
        for (int i = 0; i < nfds; i++) {
            net::utils::EpollEvent& ev = events[i];
            auto* connection = static_cast<ForgeConnection*>(ev._data);
            auto* client = connection->getClient();

            const bool peerClosed = ev._events & (net::utils::EVENT_HUP | net::utils::EVENT_RDHUP);

            // A send/recv/decode failure kills this connection, but it must not take down
            // the worker thread: an uncaught throw here escapes the std::jthread and
            // std::terminate()s the whole process. Count it as an error and leave the fd
            // disarmed.
            try {
                if (ev._events & net::utils::EVENT_OUT) {
                    client->send();
                }

                // Drain any readable data before acting on a peer close: epoll coalesces
                // readiness, so a final complete response often arrives in the same event
                // as EVENT_RDHUP/EVENT_HUP. Reading first lets us record that response
                // instead of discarding it.
                if (ev._events & net::utils::EVENT_IN) {
                    client->recv();
                }

                if (peerClosed) {
                    // The peer won't send any more: this connection is finished and leaves
                    // the pool. Record a completed response if one arrived, otherwise count
                    // the close as a connection error. Either way the connection is dropped
                    // and the fd left disarmed (it was ONESHOT).
                    if (client->isRecvComplete()) {
                        connection->advanceCycle();
                    } else {
                        errors.recordConnectionError();
                    }
                    errors.recordDroppedConnection();
                    continue;
                }

                if (client->isRecvComplete()) {
                    connection->advanceCycle();
                }

                // Re-arm one-shot with only what this connection is still waiting on.
                uint32_t interest = net::utils::EVENT_ET
                                  | net::utils::EVENT_ONESHOT
                                  | net::utils::EVENT_RDHUP
                                  | net::utils::EVENT_HUP;

                if (!client->isSendComplete()) {
                    interest |= net::utils::EVENT_OUT;
                }
                if (!client->isRecvComplete()) {
                    interest |= net::utils::EVENT_IN;
                }

                net::utils::EpollEvent rearm;
                rearm._events = interest;
                rearm._data = connection;
                if (!net::utils::epollMod(epoll, client->getSocket(), rearm)) {
                    // Can't re-arm, so this connection won't be serviced again: it has
                    // left the pool.
                    spdlog::error("failed to re-arm epoll event");
                    errors.recordDroppedConnection();
                }
            } catch (const std::exception&) {
                // The failure leaves the fd disarmed, so the connection is dropped.
                errors.recordConnectionError();
                errors.recordDroppedConnection();
            }
        }
    }
}


// Run one test load - spawns the workers threads and distributes the connections between them.
// After the workload is finished we merge the histograms and error counter and then print
// out the results
bool runTestLoad(const ForgeConfig& config, const ForgeWorkload& workload, size_t loadIndex) {
    const std::vector<std::string>& queries = workload.getQueries(loadIndex);
    const bool isWriteLoad = workload.isWriteLoad(loadIndex);

    // No queries means nothing to drive: skip this load rather than spawning workers
    // that would divide by an empty query list when advancing the query index.
    if (queries.empty()) {
        spdlog::warn("Test load {} has no queries; skipping", loadIndex);
        return true;
    }

    std::latch connectedLatch(config.getThreadCount());
    std::latch readyLatch(1);
    TimePoint deadline;
    std::vector<std::unique_ptr<ForgeWorkerThread>> threadStructs;
    std::vector<std::jthread> threads;

    // Spread the total connection count across the worker threads, handing the remainder
    // to the first few threads so the totals add up exactly to config.getConnectionCount().
    const size_t baseConnections = config.getConnectionCount() / config.getThreadCount();
    const size_t remainderConnections = config.getConnectionCount() % config.getThreadCount();

    threads.reserve(config.getThreadCount());
    for (size_t i = 0; i < config.getThreadCount(); ++i) {
        const size_t threadConnections = baseConnections + (i < remainderConnections ? 1 : 0);

        threadStructs.emplace_back(std::make_unique<ForgeWorkerThread>(&config));
        try {
            threadStructs.back()->init();
        } catch (const TuringException& e) {
            spdlog::info("Failed to init thread: {}", e.what());
            for (auto& worker : threads) {
                worker.request_stop();
            }
            readyLatch.count_down();
            return false;
        }
        threads.emplace_back(runClientThread,
                             threadStructs.back().get(),
                             &connectedLatch,
                             &readyLatch,
                             &deadline,
                             queries,
                             threadConnections,
                             isWriteLoad);
    }

    connectedLatch.wait();
    for (const auto& thread : threadStructs) {
        if (!thread->isConnected()) {
            spdlog::error("A worker failed to connect; aborting");
            for (auto& worker : threads) {
                worker.request_stop(); // set stop before releasing the gate...
            }
            readyLatch.count_down(); // ...so woken workers observe it and exit immediately
            return false;
        }
    }

    // Run for warmup + duration; the workers only record samples once the clock passes
    // measurementStart, so the warmup period warms connections/caches without polluting
    // the reported stats.
    const TimePoint runStart = Clock::now();
    const auto configWarmUpTime = std::chrono::duration<double>(config.getWarmupSeconds());
    const auto configTestDuration = std::chrono::duration<double>(config.getDurationSeconds());
    const auto warmup = std::chrono::duration_cast<Clock::duration>(configWarmUpTime);
    const auto duration = std::chrono::duration_cast<Clock::duration>(configTestDuration);
    const TimePoint measurementStart = runStart + warmup;
    deadline = measurementStart + duration;

    for (auto& thread : threadStructs) {
        thread->setMeasurementStart(measurementStart);
    }
    readyLatch.count_down();

    ForgeSpinner spinner(ForgeSpinner::parseMode(config.getAnimation()));
    spinner.run(runStart, deadline, config.getWarmupSeconds() + config.getDurationSeconds());

    for (auto& thread : threads) {
        thread.request_stop();
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Throughput is over the measurement window only (warmup excluded), matching the
    // samples that actually landed in the histograms.
    const double measurementSeconds = std::chrono::duration<double>(Clock::now() - measurementStart).count();

    // Merge per-worker histograms and error counters into the first; safe now
    // that all workers have joined.
    mergeAndPrintStats(isWriteLoad,
                       queries.size(),
                       config.getConnectionCount(),
                       measurementSeconds,
                       workload.getLabel(loadIndex),
                       threadStructs);
    return true;
}

}

int main(int argc, const char** argv) {
    argparse::ArgumentParser parser("TuringForge", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB - query load generator and latency benchmark");

    std::string server;
    std::string graphName;
    std::string queriesPath;
    std::string outputPath;
    std::string animation = "special";
    size_t threadCount = 0;
    size_t connectionCount = 0;
    size_t bufferCapacity = net::proto::DEFAULT_BUFFER_CAPACITY;
    double durationSeconds = 10.0;
    double warmupSeconds = 0.0;
    double timeoutSeconds = 0.0;

    parser.add_argument("--server")
        .metavar("ADDRESS:PORT")
        .help("Server address and port to benchmark, e.g. 127.0.0.1:6666")
        .required()
        .store_into(server);

    parser.add_argument("--threads")
        .metavar("N")
        .help("Number of worker threads (default: --connections)")
        .scan<'u', size_t>()
        .store_into(threadCount);

    parser.add_argument("--connections")
        .metavar("N")
        .help("Number of client connections to open (must be >= --threads)")
        .required()
        .scan<'u', size_t>()
        .store_into(connectionCount);

    parser.add_argument("--queries")
        .metavar("FILE")
        .help("Path to a JSON file with a top-level \"queries\" object mapping each load name to an array of query strings, or to { \"queries\": [...], \"write\": true } for a write load")
        .required()
        .store_into(queriesPath);

    parser.add_argument("--graph")
        .metavar("NAME")
        .help("Graph to run the queries against")
        .required()
        .store_into(graphName);

    parser.add_argument("--duration")
        .metavar("SECONDS")
        .help("How long to run the benchmark (default: 10)")
        .scan<'g', double>()
        .store_into(durationSeconds);

    parser.add_argument("--warmup")
        .metavar("SECONDS")
        .help("Warmup period excluded from the reported statistics (default: 0)")
        .scan<'g', double>()
        .store_into(warmupSeconds);

    parser.add_argument("--buffer-capacity")
        .metavar("BYTES")
        .help("Per-connection proto buffer capacity; should match the server (default: 1 MiB)")
        .scan<'u', size_t>()
        .store_into(bufferCapacity);

    parser.add_argument("--animation")
        .metavar("MODE")
        .help("Live animation while running: none, simple, or special (default: special)")
        .choices("none", "simple", "special")
        .store_into(animation);

    parser.add_argument("--output")
        .metavar("FILE")
        .help("Write the results to FILE instead of stdout")
        .store_into(outputPath);

    parser.add_argument("--timeout")
        .metavar("SECONDS")
        .help("Per-query timeout; queries exceeding it count as errors (default: 0 = no timeout)")
        .scan<'g', double>()
        .store_into(timeoutSeconds);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    if (!parser.is_used("--threads")) {
        threadCount = connectionCount;
    }

    if (threadCount == 0) {
        std::cerr << "--threads must be at least 1.\n";
        return EXIT_FAILURE;
    }

    if (connectionCount < threadCount) {
        std::cerr << "--connections (" << connectionCount << ") must be >= --threads ("
                  << threadCount << ").\n";
        return EXIT_FAILURE;
    }

    std::string address;
    std::string port;
    if (!parseServerAddress(server, address, port)) {
        std::cerr << "Invalid --server '" << server << "'; expected ADDRESS:PORT.\n";
        return EXIT_FAILURE;
    }

    ForgeConfig config;
    config.setAddress(address);
    config.setPort(port);
    config.setGraphName(graphName);
    config.setQueriesPath(queriesPath);
    config.setOutputPath(outputPath);
    config.setAnimation(animation);
    config.setThreadCount(threadCount);
    config.setConnectionCount(connectionCount);
    config.setBufferCapacity(bufferCapacity);
    config.setDurationSeconds(durationSeconds);
    config.setWarmupSeconds(warmupSeconds);
    config.setTimeoutSeconds(timeoutSeconds);

    // Parse the JSON workload of named test loads.
    auto workload = std::make_unique<ForgeWorkload>(config.getQueriesPath());
    try {
        workload->parseFile();
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }
    fmt::println("  test loads      {}", workload->getLoadCount());

    // Run each test load in turn; fresh workers per load reset the histograms.
    for (size_t loadIndex = 0; loadIndex < workload->getLoadCount(); ++loadIndex) {
        if (!runTestLoad(config, *workload, loadIndex)) {
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}
