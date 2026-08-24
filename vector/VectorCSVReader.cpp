#include "VectorCSVReader.h"

#include <fstream>
#include <span>
#include <sstream>
#include <string>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "Path.h"

#include "BatchVectorCreate.h"
#include "VecLibMetadata.h"
#include "VectorException.h"

using namespace vec;

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

    while (std::getline(file, line)) {
        if (line.empty()) {
            continue;
        }

        std::istringstream lineStream(line);

        if (!std::getline(lineStream, token, ',')) {
            throw VectorException("Invalid vector file format: missing ID");
        }

        const int64_t id = std::stoll(token);

        values.clear();
        while (std::getline(lineStream, token, ',')) {
            values.push_back(std::stof(token));
        }

        if (values.size() != dimension) {
            throw VectorException(fmt::format("Vector dimension mismatch: expected {}, got {}",
                                              dimension,
                                              values.size()));
        }

        batch.addPoint(id, std::span<const float>(values));
    }
}
