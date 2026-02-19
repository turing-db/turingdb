#include "CSVSourceProcessor.h"

#include <spdlog/spdlog.h>

#include "CSVParser.h"
#include "ExecutionContext.h"
#include "columns/ColumnStringTable.h"

#include "BioAssert.h"

using namespace db;

CSVSourceProcessor::CSVSourceProcessor(const fs::Path& path,
                                       bool hasHeaders,
                                       CSVErrorMode errorMode,
                                       size_t expectedFieldCount,
                                       ColumnStringTable* outputTable)
    : _path(path),
    _hasHeaders(hasHeaders),
    _errorMode(errorMode),
    _expectedFieldCount(expectedFieldCount),
    _outputTable(outputTable)
{
}

CSVSourceProcessor::~CSVSourceProcessor() {
    delete _parser;
}

CSVSourceProcessor* CSVSourceProcessor::create(PipelineV2* pipeline,
                                               const fs::Path& path,
                                               bool hasHeaders,
                                               CSVErrorMode errorMode,
                                               size_t expectedFieldCount,
                                               ColumnStringTable* outputTable) {
    auto* proc = new CSVSourceProcessor(path, hasHeaders, errorMode,
                                        expectedFieldCount, outputTable);

    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(outPort);
    proc->addOutput(outPort);
    proc->postCreate(pipeline);

    return proc;
}

std::string CSVSourceProcessor::describe() const {
    return fmt::format("CSVSourceProcessor({})", _path.get());
}

void CSVSourceProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    delete _parser;
    _parser = new CSVParser(_path, _hasHeaders, _errorMode, _expectedFieldCount);

    markAsPrepared();
}

void CSVSourceProcessor::reset() {
    delete _parser;
    _parser = new CSVParser(_path, _hasHeaders, _errorMode, _expectedFieldCount);
}

void CSVSourceProcessor::execute() {
    bioassert(_outputTable, "CSVSourceProcessor: output table not set");

    const size_t rowsRead = _parser->readChunk(_ctxt->getChunkSize(), _outputTable);

    if (rowsRead == 0) {
        if (_parser->getLinesSkipped() > 0) {
            spdlog::info("LOAD CSV completed: {} lines read, {} lines skipped",
                         _parser->getLinesRead(), _parser->getLinesSkipped());
        } else {
            spdlog::info("LOAD CSV completed: {} lines read", _parser->getLinesRead());
        }

        finish();
        return;
    }

    _output.getPort()->writeData();
}
