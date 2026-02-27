#pragma once

#include <memory>

#include "Processor.h"
#include "Path.h"
#include "CSVErrorMode.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class CSVParser;
class ColumnStringTable;

class CSVSourceProcessor : public Processor {
public:
    static CSVSourceProcessor* create(PipelineV2* pipeline,
                                      const fs::Path& path,
                                      bool hasHeaders,
                                      CSVErrorMode errorMode,
                                      size_t expectedFieldCount,
                                      ColumnStringTable* outputTable);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockOutputInterface& output() { return _output; }

private:
    fs::Path _path;
    bool _hasHeaders {false};
    CSVErrorMode _errorMode {CSVErrorMode::Fail};
    size_t _expectedFieldCount {0};
    ColumnStringTable* _outputTable {nullptr};
    PipelineBlockOutputInterface _output;

    std::unique_ptr<CSVParser> _parser;

    CSVSourceProcessor(const fs::Path& path,
                       bool hasHeaders,
                       CSVErrorMode errorMode,
                       size_t expectedFieldCount,
                       ColumnStringTable* outputTable);
    ~CSVSourceProcessor() override;
};

}
