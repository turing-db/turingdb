#include "ForgeErrorCounter.h"

#include <ostream>

#include <spdlog/fmt/fmt.h>

using namespace forge;

void ForgeErrorCounter::record(db::QueryStatus::Status status) {
    if (status == db::QueryStatus::Status::OK) {
        return;
    }

    const size_t index = static_cast<size_t>(status);
    if (index >= bucketCount) {
        return;
    }

    ++_counts[index];
    ++_total;
}

void ForgeErrorCounter::recordConnectionError() {
    ++_connectionErrors;
    ++_total;
}

void ForgeErrorCounter::recordDroppedConnection() {
    ++_droppedConnections;
}

void ForgeErrorCounter::add(const ForgeErrorCounter& other) {
    for (size_t index = 0; index < bucketCount; ++index) {
        _counts[index] += other._counts[index];
    }
    _connectionErrors += other._connectionErrors;
    _droppedConnections += other._droppedConnections;
    _total += other._total;
}

void ForgeErrorCounter::reset() {
    _counts.fill(0);
    _connectionErrors = 0;
    _droppedConnections = 0;
    _total = 0;
}

uint64_t ForgeErrorCounter::getCount(db::QueryStatus::Status status) const {
    const size_t index = static_cast<size_t>(status);
    if (index >= bucketCount) {
        return 0;
    }
    return _counts[index];
}

void ForgeErrorCounter::printStats(std::ostream& output) const {
    if (_total == 0) {
        output << "errors          none\n";
        return;
    }

    output << fmt::format("errors          {} total\n", _total);
    for (size_t index = 0; index < bucketCount; ++index) {
        if (_counts[index] == 0) {
            continue;
        }

        const auto status = static_cast<db::QueryStatus::Status>(index);
        output << fmt::format("  {:<18} {}\n", db::QueryStatusDescription::value(status), _counts[index]);
    }

    if (_connectionErrors > 0) {
        output << fmt::format("  {:<18} {}\n", "CONNECTION_ERROR", _connectionErrors);
    }
}
