// AFL++ / stdin fuzzing harness for the CSV parser.
// Writes stdin to a temp file, then parses it with CSVParser.

#include "CSVParser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "FatalException.h"

#include "Path.h"

static int fuzzOne(const char* data, size_t size) {
    if (size > 256 * 1024) {
        return 0;
    }

    // Test the static parseCSVLine on each line of input.
    std::string input(data, size);
    size_t pos = 0;
    while (pos < input.size()) {
        size_t end = input.find('\n', pos);
        if (end == std::string::npos) {
            end = input.size();
        }
        std::string line = input.substr(pos, end - pos);
        std::vector<std::string> fields;
        try {
            db::CSVParser::parseCSVLine(line, fields);
        } catch (const FatalException&) {
            throw;
        } catch (const std::exception&) {
            // Expected for malformed input.
        }
        pos = end + 1;
    }

    // Also test the full file-based parser by writing to a temp file.
    char tmpPath[] = "/tmp/fuzz_csv_XXXXXX";
    int fd = mkstemp(tmpPath);
    if (fd < 0) {
        return 0;
    }
    ssize_t written = write(fd, data, size);
    close(fd);
    if (written < 0 || static_cast<size_t>(written) != size) {
        unlink(tmpPath);
        return 0;
    }

    try {
        std::string tmpStr(tmpPath);
        fs::Path csvPath(tmpStr);
        db::CSVFileInfo info;
        db::CSVParser::peekFileStructure(csvPath, true, info);
    } catch (const FatalException&) {
        unlink(tmpPath);
        throw;
    } catch (const std::exception&) {
        // Expected for malformed input.
    }

    unlink(tmpPath);
    return 0;
}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    __AFL_INIT();
    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    return 0;
}

#else
int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    std::string input;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
