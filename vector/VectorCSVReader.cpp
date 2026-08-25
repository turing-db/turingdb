#include "VectorCSVReader.h"

#include <errno.h>
#include <fstream>
#include <span>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "Path.h"

#include "BatchVectorCreate.h"
#include "VecLibMetadata.h"
#include "VectorException.h"

using namespace vec;

namespace {

int64_t parseID(const std::string& token, size_t lineNumber) {
    const char* const start = token.c_str();
    char* end = nullptr;
    errno = 0;

    const int64_t id = strtoll(start, &end, 10);

    const bool noConversion = end == start;
    const bool outOfRange = errno == ERANGE;

    if (noConversion || outOfRange) {
        throw VectorException(fmt::format("Invalid vector ID '{}' on line {}", token, lineNumber));
    }

    return id;
}

float parseValue(const std::string& token, size_t lineNumber) {
    const char* const start = token.c_str();
    char* end = nullptr;
    errno = 0;

    const float value = strtof(start, &end);

    const bool noConversion = end == start;
    const bool outOfRange = errno == ERANGE;

    if (noConversion || outOfRange) {
        throw VectorException(fmt::format("Invalid vector value '{}' on line {}", token, lineNumber));
    }

    return value;
}

}

void VectorCSVReader::read(const fs::Path& path, BatchVectorCreate& batch) {
    std::ifstream file(path.get());
    if (!file.is_open()) {
        throw VectorException(fmt::format("Failed to open file '{}'", path.get()));
    }

    const Dimension dimension = batch.dimension();

    std::string line;
    std::string token;
    std::vector<float> values;
    values.reserve(dimension);

    size_t lineNumber = 0;

    while (std::getline(file, line)) {
        ++lineNumber;

        if (line.empty()) {
            continue;
        }

        std::istringstream lineStream(line);

        if (!std::getline(lineStream, token, ',')) {
            throw VectorException(fmt::format("Invalid vector file format: missing ID on line {}", lineNumber));
        }

        const int64_t id = parseID(token, lineNumber);

        values.clear();
        while (std::getline(lineStream, token, ',')) {
            values.push_back(parseValue(token, lineNumber));
        }

        if (values.size() != dimension) {
            throw VectorException(fmt::format("Vector dimension mismatch: expected {}, got {}",
                                              dimension,
                                              values.size()));
        }

        batch.addPoint(id, std::span<const float>(values));
    }
}
