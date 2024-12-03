#pragma once

#include "File.h"
#include "FileResult.h"

struct iocb;

struct io_event;

namespace fs {


using IORequest = struct iocb;
using IOEvent = struct io_event;
using AIOContext = uint64_t;

class AIOEngine {
public:
    static constexpr size_t DEFAULT_MAX_EVENTS = 128;

    ~AIOEngine();

    AIOEngine(const AIOEngine&) = delete;
    AIOEngine(AIOEngine&&) = delete;
    AIOEngine& operator=(const AIOEngine&) = delete;
    AIOEngine& operator=(AIOEngine&&) = delete;

    [[nodiscard]] static Result<std::unique_ptr<AIOEngine>> create(
        uint32_t maxEvents = DEFAULT_MAX_EVENTS);

    Result<void> initialize();
    Result<void> terminate();
    Result<void> wait();

    FileResult<void> submitRead(const ReadableFile auto& f,
                                std::string_view buffer,
                                size_t offset = 0);

    FileResult<void> submitWrite(const WritableFile auto& f,
                                 std::string_view content,
                                 size_t offset = 0);

private:
    AIOEngine();

    uint32_t _maxEvents = DEFAULT_MAX_EVENTS;
    std::mutex _mutex;
    AIOContext _ctxt = 0;
    uint32_t _submittedCount {};

    /* Event pool */
    std::vector<IORequest> _events;

    /* Free (not submitted) event indices */
    std::vector<size_t> _free;

    /* Event pool used in the wait() method */
    std::vector<IOEvent> _currentEvents;
};

}
