#pragma once

namespace net {
    class NetWriter;
}

namespace db {

class Block;
class ExecutionContext;

class JsonEncoderStep {
public:
    struct Tag {};

    JsonEncoderStep(net::NetWriter* writer, const Block* block)
        : _writer(writer),
        _block(block)
    {
    }

    void prepare(ExecutionContext* ctxt) {}

    inline void reset() {}

    inline bool isFinished() { return true; }

    void execute();

private:
    net::NetWriter* _writer {nullptr};
    const Block* _block {nullptr};
    bool _first {true};
};

}
