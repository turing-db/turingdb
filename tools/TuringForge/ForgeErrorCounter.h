#pragma once

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <iosfwd>

#include "QueryStatus.h"

namespace forge {

// Counts query failures bucketed by QueryStatus error type. Each worker keeps
// its own counter; they are merged (add) into one for the run report, mirroring
// the histogram flow. OK results are ignored.
class ForgeErrorCounter {
public:
    // Record one query result. OK is a no-op; any error increments its bucket.
    void record(db::QueryStatus::Status status);

    // Record a transport-level failure (connect/send/recv error, or a peer-closed
    // connection) that isn't a server-reported query status.
    void recordConnectionError();

    // Record that a connection left the pool — the server closed it, or it failed.
    // Tracked separately from errors and not counted in getTotal(): a connection
    // that closes cleanly after a completed response is attrition, not a query error.
    void recordDroppedConnection();

    void add(const ForgeErrorCounter& other);
    void reset();

    uint64_t getCount(db::QueryStatus::Status status) const;
    uint64_t getConnectionErrors() const { return _connectionErrors; }
    uint64_t getDroppedConnections() const { return _droppedConnections; }
    uint64_t getTotal() const { return _total; }
    bool isEmpty() const { return _total == 0; }

    // Print the per-error-type breakdown (only non-zero buckets) to `output`.
    void printStats(std::ostream& output) const;

private:
    static constexpr size_t bucketCount = static_cast<size_t>(db::QueryStatus::Status::_SIZE);

    std::array<uint64_t, bucketCount> _counts {};
    uint64_t _connectionErrors {0};
    uint64_t _droppedConnections {0};
    uint64_t _total {0};
};

}
