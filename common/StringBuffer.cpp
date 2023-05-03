#include "StringBuffer.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <assert.h>

namespace {

class MallocStringBuffer : public StringBuffer {
    public:
        MallocStringBuffer(char* data, size_t size) 
            : StringBuffer(data, size) 
        {
        }

        ~MallocStringBuffer() { 
            delete[] _data; 
        }

        const BufferKind getKind() const override { 
            return BufferKind::MALLOC_BUFFER; 
        }
};

class MmapStringBuffer : public StringBuffer {
    public:
        MmapStringBuffer(char* data, size_t size) 
            : StringBuffer(data, size) 
        {
        }

        ~MmapStringBuffer() {
            [[maybe_unused]] int res = munmap(_data, _size);
            assert(res == 0);
        }

        const BufferKind getKind() const override { 
            return BufferKind::MMAP_BUFFER; 
        }
};

bool shouldUseMmap(off_t fileSize) { 
    return (fileSize >= 4 * 4096); 
}

MallocStringBuffer* createMallocBuffer(int fd, size_t fileSize) {
    // Allocate a buffer to hold the file content and a terminating null character
    char* buffer = new char[fileSize + 1];
    assert(buffer != nullptr);

    // Read the file content
    const size_t chunkSize = std::min(512lu, fileSize);
    char* bufferPtr = buffer;
    size_t remainingBytes = fileSize;
    size_t bytesRead = 0;

    do {
        ssize_t res = read(fd, bufferPtr, chunkSize);
        if (res < 0) {
            delete[] buffer;
            return nullptr;
        }

        bytesRead = static_cast<size_t>(res);
        remainingBytes -= bytesRead;
        bufferPtr += bytesRead;
    } while ((bytesRead != 0) && (remainingBytes > 0));

    if (remainingBytes > 0) {
        delete[] buffer;
        return nullptr;
    }

    // Add terminating null character
    *bufferPtr = '\0';

    return new MallocStringBuffer(buffer, fileSize + 1);
}

MmapStringBuffer* createMmapBuffer(int fd, size_t fileSize) {
    // File size and null terminating character
    const size_t bufferSize = fileSize + 1;

    // Read file with mmap
    void* data = mmap(NULL, bufferSize, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    if (!data)
        return nullptr;

    // Write null terminating character
    char* buffer = static_cast<char*>(data);
    buffer[fileSize] = '\0';

    return new MmapStringBuffer(buffer, bufferSize);
}

}

StringBuffer::StringBuffer(char* data, size_t size)
    : _data(data), _size(size)
{
}

StringBuffer* StringBuffer::readFromFile(const Path& filePath) {
    // Open file descriptor to source file
    int fd = open(filePath.string().c_str(), O_RDONLY);
    if (fd < 0) {
        return nullptr;
    }

    // Get file size
    struct stat fileStat;
    if (fstat(fd, &fileStat) < 0) {
        close(fd);
        return nullptr;
    }

    if (fileStat.st_size <= 0) {
        return nullptr;
    }

    const size_t fileSize = static_cast<size_t>(fileStat.st_size);

    // Create string buffer
    StringBuffer* buf = nullptr;

    if (shouldUseMmap(fileSize)) {
        buf = createMmapBuffer(fd, fileSize);
    } else {
        buf = createMallocBuffer(fd, fileSize);
    }

    // Close file descriptor
    close(fd);

    return buf;
}
