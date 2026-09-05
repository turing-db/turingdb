#include "CSVParser.h"

#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <span>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "columns/ColumnStringTable.h"

#include "TuringException.h"

using namespace db;

namespace {

bool readOneLine(const char*& cursor, const char* end, std::string& line) {
    line.clear();

    // Skip blank lines
    while (cursor < end && (*cursor == '\n' || *cursor == '\r')) {
        if (*cursor == '\r') {
            cursor++;
            if (cursor < end && *cursor == '\n') {
                cursor++;
            }
        } else {
            cursor++;
        }
    }

    if (cursor >= end) {
        return false;
    }

    bool inQuotes = false;
    const char* start = cursor;

    while (cursor < end) {
        const char ch = *cursor;

        if (ch == '"') {
            inQuotes = !inQuotes;
            cursor++;
        } else if (!inQuotes && (ch == '\n' || ch == '\r')) {
            line.append(start, cursor - start);
            cursor++;
            if (ch == '\r' && cursor < end && *cursor == '\n') {
                cursor++;
            }
            return true;
        } else {
            cursor++;
        }
    }

    line.append(start, cursor - start);
    return !line.empty();
}

void skipBOMInline(const char*& cursor, const char* end) {
    if (end - cursor >= 3
        && static_cast<unsigned char>(cursor[0]) == 0xEF
        && static_cast<unsigned char>(cursor[1]) == 0xBB
        && static_cast<unsigned char>(cursor[2]) == 0xBF) {
        cursor += 3;
    }
}

void markLastFieldReaders(std::span<const size_t> fieldIndices, std::vector<bool>& lastReaders) {
    lastReaders.assign(fieldIndices.size(), true);

    for (size_t column = 0; column + 1 < fieldIndices.size(); column++) {
        const std::span<const size_t> laterColumns = fieldIndices.subspan(column + 1);

        lastReaders[column] = std::ranges::find(laterColumns, fieldIndices[column]) == laterColumns.end();
    }
}

} // anonymous namespace

// ---------------------------------------------------------------
// Static field parser (RFC 4180)
// ---------------------------------------------------------------

bool CSVParser::parseCSVLine(const std::string& line,
                             std::vector<std::string>& fields) {
    fields.clear();

    const char* pos = line.data();
    const char* end = pos + line.size();

    std::string field;

    while (pos <= end) {
        field.clear();

        if (pos < end && *pos == '"') {
            // Quoted field -- scan for closing quote, using bulk append
            pos++;
            const char* segStart = pos;

            bool terminated = false;
            while (pos < end) {
                if (*pos == '"') {
                    field.append(segStart, pos - segStart);
                    pos++;
                    if (pos < end && *pos == '"') {
                        // Escaped quote: ""
                        field += '"';
                        pos++;
                        segStart = pos;
                    } else {
                        terminated = true;
                        break;
                    }
                } else {
                    pos++;
                }
            }

            if (!terminated) {
                return false;
            }
        } else {
            // Unquoted field -- find comma and bulk append
            const char* segStart = pos;
            while (pos < end && *pos != ',') {
                pos++;
            }
            field.append(segStart, pos - segStart);
        }

        fields.push_back(std::move(field));

        if (pos < end && *pos == ',') {
            pos++;
            if (pos == end) {
                fields.emplace_back();
            }
        } else {
            break;
        }
    }

    return true;
}

// ---------------------------------------------------------------
// Peek file structure
// ---------------------------------------------------------------

void CSVParser::peekFileStructure(const fs::Path& path,
                                  bool hasHeaders,
                                  CSVFileInfo& info) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw TuringException(fmt::format("Cannot open CSV file: {}", path.get()));
    }

    struct stat st;
    memset(&st, 0, sizeof(st));
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        throw TuringException(fmt::format("Cannot stat CSV file: {}", path.get()));
    }

    const size_t fileSize = static_cast<size_t>(st.st_size);
    if (fileSize == 0) {
        ::close(fd);
        return;
    }

    const size_t mapSize = std::min(fileSize, static_cast<size_t>(64 * 1024));
    void* mapped = ::mmap(nullptr, mapSize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        ::close(fd);
        throw TuringException(fmt::format("Cannot mmap CSV file: {}", path.get()));
    }

    const char* cursor = static_cast<const char*>(mapped);
    const char* end = cursor + mapSize;

    skipBOMInline(cursor, end);

    std::string line;
    std::vector<std::string> fields;

    auto cleanup = [&]() {
        ::munmap(mapped, mapSize);
        ::close(fd);
    };

    if (hasHeaders) {
        if (readOneLine(cursor, end, line)) {
            if (!CSVParser::parseCSVLine(line, fields)) {
                cleanup();
                throw TuringException("Malformed CSV header line");
            }
            info._headers = fields;
            info._fieldCount = fields.size();
        }
    } else {
        if (readOneLine(cursor, end, line)) {
            if (!CSVParser::parseCSVLine(line, fields)) {
                cleanup();
                throw TuringException("Malformed first CSV line");
            }
            info._fieldCount = fields.size();
        }
    }

    cleanup();
}

// ---------------------------------------------------------------
// CSVParser
// ---------------------------------------------------------------

CSVParser::CSVParser(const fs::Path& path,
                     bool hasHeaders,
                     CSVErrorMode errorMode,
                     size_t expectedFieldCount,
                     size_t mmapChunkSize)
    : _path(path),
    _hasHeaders(hasHeaders),
    _errorMode(errorMode),
    _expectedFieldCount(expectedFieldCount),
    _mmapChunkSize(mmapChunkSize)
{
}

CSVParser::~CSVParser() {
    unmapCurrentChunk();
    if (_fd >= 0) {
        ::close(_fd);
        _fd = -1;
    }
}

void CSVParser::openFile() {
    if (_opened) {
        return;
    }
    _opened = true;

    _fd = ::open(_path.c_str(), O_RDONLY);
    if (_fd < 0) {
        throw TuringException(fmt::format("Cannot open CSV file: {}", _path.get()));
    }

    struct stat st;
    memset(&st, 0, sizeof(st));
    if (::fstat(_fd, &st) != 0) {
        ::close(_fd);
        _fd = -1;
        throw TuringException(fmt::format("Cannot stat CSV file: {}", _path.get()));
    }

    _fileSize = static_cast<size_t>(st.st_size);
    if (_fileSize == 0) {
        _finished = true;
        return;
    }

    mapNextChunk();
    skipBOM();

    // Consume header line so data reading starts after it
    if (_hasHeaders) {
        readLine();
    }
}

void CSVParser::mapNextChunk() {
    unmapCurrentChunk();

    if (_fileOffset >= _fileSize) {
        _finished = true;
        return;
    }

    const size_t remaining = _fileSize - _fileOffset;
    _mappedSize = std::min(_mmapChunkSize, remaining);
    _mappedOffset = _fileOffset;

    _mappedPtr = ::mmap(nullptr, _mappedSize, PROT_READ, MAP_PRIVATE, _fd,
                        static_cast<off_t>(_mappedOffset));
    if (_mappedPtr == MAP_FAILED) {
        _mappedPtr = nullptr;
        throw TuringException(fmt::format("Failed to mmap CSV file: {}", _path.get()));
    }

    _cursor = static_cast<const char*>(_mappedPtr);
    _end = _cursor + _mappedSize;
    _fileOffset += _mappedSize;
}

void CSVParser::unmapCurrentChunk() {
    if (!_mappedPtr) {
        return;
    }

    ::munmap(_mappedPtr, _mappedSize);
    _mappedPtr = nullptr;
    _cursor = nullptr;
    _end = nullptr;
    _mappedSize = 0;
}

void CSVParser::skipBOM() {
    if (_cursor && (_end - _cursor) >= 3
        && static_cast<unsigned char>(_cursor[0]) == 0xEF
        && static_cast<unsigned char>(_cursor[1]) == 0xBB
        && static_cast<unsigned char>(_cursor[2]) == 0xBF) {
        _cursor += 3;
    }
}

bool CSVParser::readLine() {
    _lineBuffer.clear();

    // Skip blank lines
    while (!_finished) {
        if (_cursor >= _end) {
            if (_fileOffset >= _fileSize) {
                _finished = true;
                break;
            }
            mapNextChunk();
            continue;
        }
        if (*_cursor != '\n' && *_cursor != '\r') {
            break;
        }
        if (*_cursor == '\r') {
            _cursor++;
            if (_cursor < _end && *_cursor == '\n') {
                _cursor++;
            }
        } else {
            _cursor++;
        }
    }

    if (_finished) return false;

    // Scan for end of line, respecting quoted fields
    bool inQuotes = false;

    while (true) {
        const char* start = _cursor;

        while (_cursor < _end) {
            const char ch = *_cursor;
            if (ch == '"') {
                inQuotes = !inQuotes;
                _cursor++;
            } else if (!inQuotes && (ch == '\n' || ch == '\r')) {
                _lineBuffer.append(start, _cursor - start);
                _cursor++;
                if (ch == '\r' && _cursor < _end && *_cursor == '\n') {
                    _cursor++;
                }
                return true;
            } else {
                _cursor++;
            }
        }

        // Reached end of chunk without finding line end
        _lineBuffer.append(start, _cursor - start);

        if (_fileOffset >= _fileSize) {
            _finished = true;
            return !_lineBuffer.empty();
        }

        mapNextChunk();
    }
}

CSVParser::RecordStatus CSVParser::readRecord() {
    if (!readLine()) {
        return RecordStatus::Finished;
    }

    _linesRead++;

    if (!parseCSVLine(_lineBuffer, _fields)) {
        if (_errorMode == CSVErrorMode::Skip) {
            _linesSkipped++;
            spdlog::warn("CSV line {}: unterminated quote, skipping", _linesRead);
            return RecordStatus::Skipped;
        }

        throw TuringException(fmt::format("CSV line {}: unterminated quote", _linesRead));
    }

    if (_fields.size() != _expectedFieldCount) {
        if (_errorMode == CSVErrorMode::Skip) {
            _linesSkipped++;
            spdlog::warn("CSV line {}: expected {} fields, got {}, skipping",
                         _linesRead, _expectedFieldCount, _fields.size());
            return RecordStatus::Skipped;
        }

        throw TuringException(fmt::format("CSV line {}: expected {} fields, got {}",
                                          _linesRead, _expectedFieldCount, _fields.size()));
    }

    return RecordStatus::Read;
}

size_t CSVParser::readChunk(size_t maxRows, ColumnStringTable* output) {
    if (!_opened) {
        openFile();
    }
    if (_finished) {
        return 0;
    }

    output->clear();

    size_t rowsRead = 0;

    while (rowsRead < maxRows) {
        const RecordStatus status = readRecord();
        if (status == RecordStatus::Finished) {
            break;
        } else if (status == RecordStatus::Skipped) {
            continue;
        }

        for (size_t i = 0; i < _fields.size(); i++) {
            output->getFieldColumn(i)->push_back(std::move(_fields[i]));
        }
        rowsRead++;
    }

    return rowsRead;
}

size_t CSVParser::readChunk(size_t maxRows,
                            std::span<const size_t> fieldIndices,
                            ColumnStringTable* output) {
    if (!_opened) {
        openFile();
    }
    if (_finished) {
        return 0;
    }

    output->clear();

    // Two accesses to one field - row[0] beside row.name - resolve to the same index, so
    // a column takes the field's characters only when no later column reads it too.
    std::vector<bool> lastReaders;
    markLastFieldReaders(fieldIndices, lastReaders);

    size_t rowsRead = 0;

    while (rowsRead < maxRows) {
        const RecordStatus status = readRecord();
        if (status == RecordStatus::Finished) {
            break;
        } else if (status == RecordStatus::Skipped) {
            continue;
        }

        for (size_t i = 0; i < fieldIndices.size(); i++) {
            std::string& field = _fields[fieldIndices[i]];
            ColumnStringTable::StringColumn* const column = output->getFieldColumn(i);

            if (lastReaders[i]) {
                column->push_back(std::move(field));
            } else {
                column->push_back(field);
            }
        }
        rowsRead++;
    }

    return rowsRead;
}
