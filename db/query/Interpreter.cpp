#include "Interpreter.h"

#include <string.h>
#include <string_view>

#include "QueryParser.h"
#include "plan/Planner.h"
#include "plan/PullPlan.h"

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

}

Interpreter::Interpreter(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

Interpreter::~Interpreter() {
}

void Interpreter::execQuery(StringSpan query, Buffer* outBuffer) {
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

        // End
        strcpy(writer.getBuffer(), bodyEnd.c_str());
        writer.setWrittenBytes(bodyEnd.size());
    }

    // Cleanup
    delete pullPlan;

    return;
}

void Interpreter::handleQueryError(QueryStatus status, Buffer* outBuffer) {
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
