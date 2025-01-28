#pragma once

#include "BioAssert.h"
#include "DataPartSpan.h"

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <span>

namespace db {

class DataPart;
class GraphLoader;

class DataPartManager {
public:
    using DataPartArray = std::array<std::unique_ptr<DataPart>, 1024>;

    DataPartManager(const DataPartManager&) = delete;
    DataPartManager(DataPartManager&&) = delete;
    DataPartManager& operator=(const DataPartManager&) = delete;
    DataPartManager& operator=(DataPartManager&&) = delete;

    DataPartManager();
    ~DataPartManager();

    DataPartSpan getView() const {
        return std::span(_parts).subspan(0, _occupied.load());
    }

    DataPartSpan getView(size_t count) const {
        bioassert(count <= _occupied.load());
        return std::span(_parts).subspan(0, count);
    }

    size_t getCount() const { return _occupied.load(); }

    void store(std::unique_ptr<DataPart> part) {
        std::unique_lock lock(_mutex);
        msgbioassert(_occupied.load() < MAX_DATAPART_COUNT, "Too many dataparts");
        _parts[_occupied.load()] = std::move(part);
        _occupied.fetch_add(1);
    }

private:
    friend GraphLoader;

    static constexpr size_t MAX_DATAPART_COUNT = 1024;
    DataPartArray _parts;
    std::mutex _mutex;
    std::atomic<size_t> _occupied = 0;
};

}
