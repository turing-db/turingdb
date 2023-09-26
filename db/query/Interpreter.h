#pragma once

#include <string>

class Buffer;

namespace db {

class InterpreterContext;

class Interpreter {
public:
    explicit Interpreter(InterpreterContext* interpCtxt);
    ~Interpreter();

    bool execQuery(const std::string& query, Buffer* outBuffer);

private:
    InterpreterContext* _interpCtxt {nullptr};
    std::string headerOk =
        "HTTP/1.1 200 OK\r\n";
    std::string emptyLine = "\r\n";
    std::string body = "{\"data\":[],\"errors\":[]}";
};

}
