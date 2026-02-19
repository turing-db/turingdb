#pragma once

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
    bool _hasHeaders;
    CSVErrorMode _errorMode;
    size_t _expectedFieldCount;
    PipelineBlockOutputInterface _output;

    ColumnStringTable* _outputTable {nullptr};
    CSVParser* _parser {nullptr};

    CSVSourceProcessor(const fs::Path& path,
                       bool hasHeaders,
                       CSVErrorMode errorMode,
                       size_t expectedFieldCount,
                       ColumnStringTable* outputTable);
    ~CSVSourceProcessor() override;
};

}
