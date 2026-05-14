#include <stdlib.h>

#include <span>
#include <string>
#include <vector>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "Path.h"

using namespace db;

class CountingVisitor : public ParquetSaxVisitor {
public:
    explicit CountingVisitor(size_t previewRowsCount)
        : _previewRowsCount(previewRowsCount)
    {
    }

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const parquet::SchemaDescriptor* schema = metadata.schema();
        const size_t numColumns = static_cast<size_t>(metadata.num_columns());

        _columnNames.reserve(numColumns);
        for (size_t c = 0; c < numColumns; ++c) {
            _columnNames.emplace_back(schema->Column(static_cast<int>(c))->name());
        }
        _preview.assign(numColumns, {});
        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0) {
            _totalRows += values.size();
        }
        for (const int64_t v : values) {
            if (_preview[columnIndex].size() < _previewRowsCount) {
                _preview[columnIndex].push_back(fmt::format("{}", v));
            }
        }
        return true;
    }

    bool onDoubleValues(size_t columnIndex, std::span<const double> values) override {
        if (columnIndex == 0) {
            _totalRows += values.size();
        }
        for (const double v : values) {
            if (_preview[columnIndex].size() < _previewRowsCount) {
                _preview[columnIndex].push_back(fmt::format("{}", v));
            }
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        if (columnIndex == 0) {
            _totalRows += values.size();
        }
        for (const auto& byteArray : values) {
            if (_preview[columnIndex].size() < _previewRowsCount) {
                _preview[columnIndex].emplace_back(
                    reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
            }
        }
        return true;
    }

    bool onFileEnd() override {
        fmt::println("  total rows: {}", _totalRows);
        fmt::println("  first {} row(s):", _previewRowsCount);

        for (size_t r = 0; r < _previewRowsCount; ++r) {
            for (size_t c = 0; c < _preview.size(); ++c) {
                if (r >= _preview[c].size()) {
                    continue;
                }
                fmt::print("    {} = {}", _columnNames[c], _preview[c][r]);
                if (c + 1 < _preview.size()) {
                    fmt::print(",");
                }
            }
            fmt::println("");
        }
        return true;
    }

private:
    size_t _previewRowsCount {0};
    size_t _totalRows {0};
    std::vector<std::string> _columnNames;
    std::vector<std::vector<std::string>> _preview;
};

static void processParquetFile(const fs::Path& path, size_t previewRowsCount) {
    fmt::println("Reading {}", path.get());

    CountingVisitor visitor(previewRowsCount);
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }

    fmt::println("");
}

int main(int argc, const char** argv) {
    constexpr size_t previewRowsCount = 5;

    const fs::Path nodesPath(SAMPLE_DIR "/nodes.parquet");
    const fs::Path edgesPath(SAMPLE_DIR "/edges.parquet");

    processParquetFile(nodesPath, previewRowsCount);
    processParquetFile(edgesPath, previewRowsCount);

    return EXIT_SUCCESS;
}
