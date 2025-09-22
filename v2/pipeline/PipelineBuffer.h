#pragma once

namespace db {
class Column;
}

namespace db::v2 {

class Processor;
class PipelineV2;

class PipelineBuffer {
public:
    friend PipelineV2;

    Processor* getSource() const { return _source; }
    Processor* getConsumer() const { return _consumer; }

    bool hasData() const { return _hasData; }
    bool canWriteData() const { return !_hasData; }

    Column* consume() {
        _hasData = false;
        return _col;
    }

    void writeData() {
        _hasData = true;
    }

    Column* getColumn() const { return _col; }
    void setColumn(Column* column) { _col = column; }

    void setSource(Processor* source) { _source = source; }
    void setConsumer(Processor* consumer) { _consumer = consumer; }

    static PipelineBuffer* create(PipelineV2* pipeline);

private:
    Processor* _source {nullptr};
    Processor* _consumer {nullptr};
    Column* _col {nullptr};
    bool _hasData {false};

    PipelineBuffer() = default;
    ~PipelineBuffer() = default;
};

}
