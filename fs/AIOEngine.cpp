#include "AIOEngine.h"
#include "BioAssert.h"

#include <mutex>
#include <numeric>
#include <sys/syscall.h>
#include <linux/aio_abi.h>

using namespace fs;

static_assert(std::same_as<fs::AIOContext, aio_context_t>);

long io_setup(unsigned nr, aio_context_t* ctxp) {
    return syscall(__NR_io_setup, nr, ctxp);
}

long io_destroy(aio_context_t ctx) {
    return syscall(__NR_io_destroy, ctx);
}

long io_submit(aio_context_t ctx, long nr, struct iocb** iocbpp) {
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

long io_getevents(aio_context_t ctx,
                  long min_nr,
                  long nr,
                  struct io_event* events,
                  struct timespec* timeout) {
    return syscall(__NR_io_getevents, ctx, min_nr, nr, events, timeout);
}


AIOEngine::AIOEngine() = default;

AIOEngine::~AIOEngine() {
    if (_ctxt != 0) {
        terminate();
    }
}

Result<std::unique_ptr<AIOEngine>> AIOEngine::create(uint32_t maxEvents) {
    std::unique_ptr<AIOEngine> engine(new AIOEngine);
    engine->_maxEvents = maxEvents;

    if (auto res = engine->initialize(); !res) {
        return res.get_unexpected();
    }

    return engine;
}

Result<void> AIOEngine::initialize() {
    _events.resize(_maxEvents);
    _free.resize(_maxEvents);
    _currentEvents.resize(_maxEvents);

    std::iota(_free.begin(), _free.end(), 0);

    if (::io_setup((int)_maxEvents, &_ctxt) < 0) {
        _ctxt = 0;
        return Error::result("Could not initialize AIO Context: {}", strerror(errno));
    }

    return {};
}

Result<void> AIOEngine::terminate() {
    auto res = wait();
    if (auto res = wait(); !res) {
        return res;
    }

    std::unique_lock lock(_mutex);
    if (auto err = ::io_destroy(_ctxt); err < 0) {
        res = Error::result("Could not destroy AIO Context: {}", strerror(errno));
        return res;
    }

    _ctxt = -1;
    return {};
}

Result<void> AIOEngine::wait() {
    std::unique_lock lock(_mutex);

    int64_t nEvents = 0;
    while ((uint32_t)nEvents < _submittedCount) {
        nEvents = ::io_getevents(_ctxt, 1, _maxEvents, _currentEvents.data(), nullptr);

        if (nEvents < 0) {
            return Error::result("Could not wait for AIO events: {}", strerror(errno));
        }

        for (uint32_t i = 0; i < nEvents; i++) {
            const uint64_t id = static_cast<size_t>(_currentEvents[i].data);
            _free.push_back(id);
            _submittedCount--;
        }
    }

    return {};
}

FileResult<void> AIOEngine::submitRead(const ReadableFile auto& f,
                                       std::string_view buffer,
                                       size_t offset) {
    bioassert(!_events.empty());
    std::unique_lock lock(_mutex);

    if (_free.empty()) {
        // TODO Might want to wait here instead of return error
        // This requires a shared_lock first though (then a unique lock), which
        // might be expensive
        return FileError::result(f.getPath().get(), "Submitted to many AIO jobs");
    }

    const size_t id = _free.back();
    IORequest* ev = &_events.at(id);
    ev->aio_fildes = f.getDescriptor();
    ev->aio_lio_opcode = IOCB_CMD_PREAD;
    ev->aio_buf = (uint64_t)buffer.data();
    ev->aio_nbytes = buffer.size();
    ev->aio_offset = offset;
    ev->aio_data = id;

    if (int res = ::io_submit(_ctxt, 1, &ev); res < 0) {
        return FileError::result(f.getPath().get(), "Could not submit read job: {}", strerror(errno));
    }

    _free.pop_back();
    _submittedCount++;

    return {};
}

FileResult<void> AIOEngine::submitWrite(const WritableFile auto& f,
                                        std::string_view content,
                                        size_t offset) {
    bioassert(!_events.empty());
    std::unique_lock lock(_mutex);

    if (_free.empty()) {
        return FileError::result(f.getPath().get(), "Submitted to many AIO jobs");
    }

    IORequest* ev = &_events.at(_free.back());
    ev->aio_fildes = f.getDescriptor();
    ev->aio_lio_opcode = IOCB_CMD_PWRITE;
    ev->aio_buf = (uint64_t)content.data();
    ev->aio_nbytes = content.size();
    ev->aio_offset = offset;

    if (int res = ::io_submit(_ctxt, 1, &ev); res < 0) {
        return FileError::result(f.getPath().get(), "Could not submit write job: {}", strerror(errno));
    }

    _free.pop_back();
    _submittedCount++;

    return {};
}

template FileResult<void> AIOEngine::submitRead(const IFile&, std::string_view, size_t);
template FileResult<void> AIOEngine::submitRead(const IOFile&, std::string_view, size_t);
template FileResult<void> AIOEngine::submitWrite(const IOFile&, std::string_view, size_t);
