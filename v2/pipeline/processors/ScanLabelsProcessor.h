#pragma once

#include <memory>

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {
class ScanLabelsChunkWriter;
}

namespace db::v2 {

class ScanLabelsProcessor : public Processor {
public:
    static ScanLabelsProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    void setIDsTag(ColumnTag tag) { _idsTag = tag; }
    void setNamesTag(ColumnTag tag) { _namesTag = tag; }
    ColumnTag getIDsTag() const { return _idsTag; }
    ColumnTag getNamesTag() const { return _namesTag; }

    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockOutputInterface _output;
    std::unique_ptr<ScanLabelsChunkWriter> _it;
    ColumnTag _idsTag;
    ColumnTag _namesTag;

    ScanLabelsProcessor();
    ~ScanLabelsProcessor();
};

}
