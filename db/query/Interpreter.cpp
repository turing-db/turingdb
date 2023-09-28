#include "Interpreter.h"

#include <string.h>
#include <string_view>
#include <stdio.h>

#include "QueryParser.h"
#include "plan/Planner.h"
#include "plan/PullPlan.h"
#include "SymbolTable.h"

#include "Buffer.h"

using namespace db;

namespace {

std::string_view getQueryStatusString(QueryStatus status) {
    switch (status) {
        case QueryStatus::OK:
            return std::string_view("OK", 2);

        case QueryStatus::PARSE_ERROR:
            return std::string_view("PARSE_ERROR", 11);

        case QueryStatus::QUERY_PLAN_ERROR:
            return std::string_view("PLAN_ERROR", 10);

        case QueryStatus::EXEC_ERROR:
            return std::string_view("EXEC_ERROR", 10);

        default:
            return std::string_view("UNKNOWN_ERROR", 13);
    }
}

void writeObjectBegin(Buffer::Writer writer) {
    char* buffer = writer.getBuffer();
    *buffer = '{';
    writer.setWrittenBytes(1);
}

void writeObjectEnd(Buffer::Writer writer) {
    char* buffer = writer.getBuffer();
    *buffer = '}';
    writer.setWrittenBytes(1);
}

void writeSymbol(const Symbol* sym, Buffer::Writer writer) {
    char* buffer = writer.getBuffer();
    size_t bytesWritten = 0;

    {
        *buffer = '"';
        bytesWritten++;
        buffer++;
    }

    {
        const std::string& symName = sym->getName();
        strcpy(buffer, symName.c_str());
        const size_t symSize = symName.size();
        bytesWritten += symSize;
        buffer += symSize;
    }

    {
        *buffer = '"';
        bytesWritten++;
        buffer++;
    }
    {
        *buffer = ':';
        bytesWritten++;
        buffer++;
    }
    
    writer.setWrittenBytes(bytesWritten);
}

void writeValue(const Value& value, Buffer::Writer writer) {
    char* buffer = writer.getBuffer();
    size_t bytes = 0;

    switch (value.getType().getKind()) {
        case ValueType::VK_INT:
        {
            bytes = sprintf(buffer, "%li", value.getInt());
        }
        break;

        case ValueType::VK_UNSIGNED:
        {
            bytes = sprintf(buffer, "%lu", value.getUint());
        }
        break;

        case ValueType::VK_BOOL:
        {
            if (value.getBool()) {
                strcpy(buffer, "true");
                bytes = 4;
            } else {
                strcpy(buffer, "false");
                bytes = 5;
            }
        }
        break;

        case ValueType::VK_DECIMAL:
        {
            bytes = sprintf(buffer, "%lf", value.getDouble());
        }
        break;

        case ValueType::VK_STRING_REF:
        {
            const auto strRef = value.getStringRef();
            const auto size = strRef.size();
            *buffer = '\"';
            buffer++;
            strcpy(buffer, strRef.begin());
            buffer += size;
            *buffer = '"';
            buffer++;
            bytes = size+2;
        }
        break;

        case ValueType::VK_STRING:
        {
            const std::string& str = value.getString();
            const auto size = str.size();
            *buffer = '\"';
            buffer++;
            strcpy(buffer, str.data());
            buffer += size;
            *buffer = '"';
            buffer++;
            bytes = size+2;
        }
        break;

        default:
        {
            strcpy(buffer, "null");
            bytes = 4;
        }
        break;
    }

    writer.setWrittenBytes(bytes);
}

}

Interpreter::Interpreter(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

Interpreter::~Interpreter() {
}

void Interpreter::execQuery(StringSpan query, Buffer* outBuffer) const {
    QueryParser parser;
    QueryCommand* cmd = parser.parse(query);
    if (!cmd) {
        return handleQueryError(QueryStatus::PARSE_ERROR, outBuffer);
    }

    Planner planner(_interpCtxt);
    PullPlan* pullPlan = planner.makePlan(cmd);
    if (!pullPlan) {
        return handleQueryError(QueryStatus::QUERY_PLAN_ERROR, outBuffer);
    }

    {
        auto writer = outBuffer->getWriter();

        // Write http response header
        strcpy(writer.getBuffer(), headerOk.c_str());
        writer.setWrittenBytes(headerOk.size()); 
        strcpy(writer.getBuffer(), emptyLine.c_str());
        writer.setWrittenBytes(emptyLine.size());

        // Begin
        strcpy(writer.getBuffer(), bodyBegin.c_str());
        writer.setWrittenBytes(bodyBegin.size());

        // Status
        const std::string_view statusStr = getQueryStatusString(QueryStatus::OK);
        strcpy(writer.getBuffer(), statusStr.data());
        writer.setWrittenBytes(statusStr.size());

        // Post status
        strcpy(writer.getBuffer(), bodyPostStatus.c_str());
        writer.setWrittenBytes(bodyPostStatus.size());

        const SymbolTable* symTable = pullPlan->getSymbolTable();
        while (pullPlan->pull() != PullStatus::DONE) {
            const Frame& frame = pullPlan->getFrame();

            writeObjectBegin(writer);

            for (const Symbol* symbol : symTable->symbols()) {
                const Value& value = frame[symbol];
                writeSymbol(symbol, writer);
                writeValue(value, writer);
            }

            writeObjectEnd(writer);
        }

        // End
        strcpy(writer.getBuffer(), bodyEnd.c_str());
        writer.setWrittenBytes(bodyEnd.size());
    }

    // Cleanup
    delete pullPlan;

    return;
}

void Interpreter::handleQueryError(QueryStatus status, Buffer* outBuffer) const {
    auto writer = outBuffer->getWriter();

    // Write http response header
    strcpy(writer.getBuffer(), headerOk.c_str());
    writer.setWrittenBytes(headerOk.size()); 
    strcpy(writer.getBuffer(), emptyLine.c_str());
    writer.setWrittenBytes(emptyLine.size());

    // Begin
    strcpy(writer.getBuffer(), bodyBegin.c_str());
    writer.setWrittenBytes(bodyBegin.size());

    // Status
    const std::string_view statusStr = getQueryStatusString(status);
    strcpy(writer.getBuffer(), statusStr.data());
    writer.setWrittenBytes(statusStr.size());

    // Post status
    strcpy(writer.getBuffer(), bodyPostStatus.c_str());
    writer.setWrittenBytes(bodyPostStatus.size());

    // End
    strcpy(writer.getBuffer(), bodyEnd.c_str());
    writer.setWrittenBytes(bodyEnd.size());
}
