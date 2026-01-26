#include "LoadVectorProcessor.h"

#include <fstream>
#include <sstream>
#include <span>

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "ExecutionContext.h"
#include "VectorDatabase.h"
#include "VecLibWriteAccessor.h"
#include "BatchVectorCreate.h"
#include "PipelineException.h"
#include "VectorResult.h"

using namespace db;

LoadVectorProcessor::LoadVectorProcessor(std::string_view filePath,
                                         std::string_view indexName)
    : _filePath(filePath),
    _indexName(indexName)
{
}

LoadVectorProcessor::~LoadVectorProcessor() {
}

LoadVectorProcessor* LoadVectorProcessor::create(PipelineV2* pipeline,
                                                 std::string_view filePath,
                                                 std::string_view indexName) {
    LoadVectorProcessor* proc = new LoadVectorProcessor(filePath, indexName);

    PipelineOutputPort* outCount = PipelineOutputPort::create(pipeline, proc);
    proc->_outCount.setPort(outCount);
    proc->addOutput(outCount);

    proc->postCreate(pipeline);

    return proc;
}

std::string LoadVectorProcessor::describe() const {
    return fmt::format("LoadVectorProcessor @={}", fmt::ptr(this));
}

void LoadVectorProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void LoadVectorProcessor::reset() {
}

void LoadVectorProcessor::execute() {
    vec::VectorDatabase* vectorDb = _ctxt->getVectorDatabase();
    if (!vectorDb) {
        throw PipelineException("VectorDatabase not available");
    }

    // Get write accessor for exclusive access
    vec::VecLibWriteAccessor accessor = vectorDb->getLibraryForWrite(std::string(_indexName));
    if (!accessor.isValid()) {
        throw PipelineException(fmt::format("Vector index '{}' not found", _indexName));
    }

    // Parse file and load embeddings
    // Expected format: CSV with id,dim1,dim2,...,dimN
    const std::string filePath{_filePath};
    std::ifstream file{filePath};
    if (!file.is_open()) {
        throw PipelineException(fmt::format("Failed to open file '{}'", _filePath));
    }

    vec::Dimension dimension = accessor.metadata()._dimension;
    vec::BatchVectorCreate batch;
    accessor.prepareCreateBatch(&batch);

    std::string line;
    std::string token;
    std::vector<float> values;
    values.reserve(dimension);

    for (;;) {
        const bool hasLine = static_cast<bool>(std::getline(file, line));
        if (!hasLine) {
            break;
        }
        if (line.empty()) {
            continue;
        }

        std::istringstream iss(line);

        // Read ID
        if (!std::getline(iss, token, ',')) {
            throw PipelineException("Invalid vector file format: missing ID");
        }
        const uint64_t id = std::stoull(token);

        // Read vector values
        values.clear();

        while (std::getline(iss, token, ',')) {
            values.push_back(std::stof(token));
        }

        if (values.size() != dimension) {
            throw PipelineException(fmt::format(
                "Vector dimension mismatch: expected {}, got {}",
                dimension, values.size()));
        }

        batch.addPoint(id, std::span<const float>(values));
    }

    const vec::VectorResult<void> result = accessor.addEmbeddings(batch);
    if (!result.has_value()) {
        throw PipelineException(fmt::format(
            "Failed to add embeddings: {}", result.error().fmtMessage()));
    }

    using ColumnUInt = ColumnConst<types::UInt64::Primitive>;
    ColumnUInt* colCount = _outCount.getValue()->as<ColumnUInt>();
    colCount->set(batch.count());

    _outCount.getPort()->writeData();
    finish();
}
