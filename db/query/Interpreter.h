#pragma once

#include <string>

#include "StringSpan.h"
#include "QueryStatus.h"

class Buffer;

namespace db {

class InterpreterContext;

class Interpreter {
public:
    explicit Interpreter(InterpreterContext* interpCtxt);
    ~Interpreter();

    void execQuery(StringSpan query, Buffer* outBuffer);

private:
    InterpreterContext* _interpCtxt {nullptr};
    const std::string headerOk =
        "HTTP/1.1 200 OK\r\n";
    const std::string emptyLine = "\r\n";
    const std::string bodyBegin = "{\"status\":\"";
    const std::string bodyPostStatus = "\",\"data\":[";
    const std::string bodyEnd = "]}\n";

    void handleQueryError(QueryStatus status, Buffer* outBuffer);
};

}
