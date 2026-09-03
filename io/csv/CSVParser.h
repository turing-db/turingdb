#pragma once

#include <span>
#include <string>
#include <vector>

#include "Path.h"
#include "CSVErrorMode.h"
#include "CSVFileInfo.h"

namespace db {

class ColumnStringTable;

class CSVParser {
public:
    static constexpr size_t DEFAULT_MMAP_CHUNK_SIZE = 512 * 1024 * 1024;

    CSVParser(const fs::Path& path,
              bool hasHeaders,
              CSVErrorMode errorMode,
              size_t expectedFieldCount,
              size_t mmapChunkSize = DEFAULT_MMAP_CHUNK_SIZE);
    ~CSVParser();

    CSVParser(const CSVParser&) = delete;
    CSVParser& operator=(const CSVParser&) = delete;

    // Read up to maxRows into output. Returns number of rows read (0 at EOF).
    size_t readChunk(size_t maxRows, ColumnStringTable* output);

    // Read up to maxRows into output, keeping only the fields fieldIndices names:
    // field fieldIndices[i] of each record fills output's field column i, so output
    // holds one field column per entry of fieldIndices. Returns rows read (0 at EOF).
    size_t readChunk(size_t maxRows,
                     std::span<const size_t> fieldIndices,
                     ColumnStringTable* output);

    size_t getLinesRead() const { return _linesRead; }
    size_t getLinesSkipped() const { return _linesSkipped; }

    // Parse a single CSV line into fields per RFC 4180.
    // Returns false on malformed input (unterminated quote).
    static bool parseCSVLine(const std::string& line,
                              std::vector<std::string>& fields);

    // Peek at a CSV file to discover its structure (field count, headers).
    static void peekFileStructure(const fs::Path& path,
                                  bool hasHeaders,
                                  CSVFileInfo& info);

private:
    // What pulling one record off the file left in _fields: a record to store, one the
    // error mode dropped, or nothing because the file is exhausted
    enum class RecordStatus {
        Read,
        Skipped,
        Finished,
    };

    fs::Path _path;
    bool _hasHeaders {false};
    CSVErrorMode _errorMode {CSVErrorMode::Fail};
    size_t _expectedFieldCount {0};
    size_t _mmapChunkSize {0};

    int _fd {-1};
    size_t _fileSize {0};
    size_t _fileOffset {0};

    void* _mappedPtr {nullptr};
    size_t _mappedSize {0};
    size_t _mappedOffset {0};

    const char* _cursor {nullptr};
    const char* _end {nullptr};

    bool _opened {false};
    bool _finished {false};

    size_t _linesRead {0};
    size_t _linesSkipped {0};

    std::string _lineBuffer;
    std::vector<std::string> _fields;

    void openFile();

    // Parse the next record into _fields, reporting whether it is one to store. Throws
    // on a malformed record under CSVErrorMode::Fail.
    RecordStatus readRecord();

    void mapNextChunk();
    void unmapCurrentChunk();
    bool readLine();
    void skipBOM();
};

}
