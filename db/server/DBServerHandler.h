#pragma once

#include <string>

class Buffer;

namespace db {

class Interpreter;

class DBServerHandler {
public:
    DBServerHandler(Interpreter* interp)
        : _interp(interp)
    {
    }

    void process(Buffer* outBuffer, const std::string& uri);

private:
    Interpreter* _interp {nullptr};
};

}
