#include "DebugDumpStep.h"

#include "JsonEncodingUtils.h"

using namespace db;

struct StreamWriter {
    void write(char c) {
        _out << c;
    }

    void write(std::string_view str) {
        _out << str;
    }

    std::ostream& _out;
};

void DebugDumpStep::execute() {
    StreamWriter writer{_out};
    JsonEncodingUtils<StreamWriter>::writeBlock(_input, &writer);
}
