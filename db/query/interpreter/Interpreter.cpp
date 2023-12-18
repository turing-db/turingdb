#include "Interpreter.h"

#include <string_view>
#include <chrono>

#include "ASTContext.h"
#include "QueryParser.h"
#include "QueryAnalyzer.h"
#include "Planner.h"
#include "QueryPipelineBuilder.h"
#include "QueryPipeline.h"

#include "Buffer.h"

using namespace db;

using Clock = std::chrono::system_clock;

namespace {

static const std::string Ok = "OK";
static const std::string ParseError = "PARSE_ERROR";
static const std::string AnalyzeError = "ANALYZE_ERROR";
static const std::string PlanError = "PLAN_ERROR";
static const std::string ExecError = "EXEC_ERROR";
static const std::string UnknownError = "UNKNOWN_ERROR";
static const std::string BufferOverflow = "BUFFER_OVERFLOW";

const std::string& getQueryStatusString(QueryStatus status) {
    switch (status) {
        case QueryStatus::OK:
            return Ok;

        case QueryStatus::PARSE_ERROR:
            return ParseError;

        case QueryStatus::ANALYZE_ERROR:
            return AnalyzeError;

        case QueryStatus::QUERY_PLAN_ERROR:
            return PlanError;

        case QueryStatus::EXEC_ERROR:
            return ExecError;

        case QueryStatus::BUFFER_OVERFLOW:
            return BufferOverflow;

        default:
            return UnknownError;
    }
}

}

Interpreter::Interpreter(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

Interpreter::~Interpreter() {
}

void Interpreter::execQuery(StringSpan query, Buffer* outBuffer) const {
    const auto timeExecStart = Clock::now();

    ASTContext astCtxt;
    QueryParser parser(&astCtxt);
    QueryCommand* cmd = parser.parse(query);
    if (!cmd) {
        return handleQueryError(QueryStatus::PARSE_ERROR, outBuffer);
    }

    QueryAnalyzer analyzer(&astCtxt);
    if (!analyzer.analyze(cmd)) {
        return handleQueryError(QueryStatus::ANALYZE_ERROR, outBuffer);
    }

    Planner planner(cmd, _interpCtxt);
    if (!planner.buildQueryPlan()) {
        return handleQueryError(QueryStatus::QUERY_PLAN_ERROR, outBuffer);
    }
    
    const auto timeExecEnd = Clock::now();
    const std::chrono::duration<double, std::milli> duration = timeExecEnd - timeExecStart;

    const bool writeRes = writeResponse(outBuffer, duration.count());
    if (!writeRes) {
        handleQueryError(QueryStatus::BUFFER_OVERFLOW, outBuffer);
    }
}

bool Interpreter::writeResponse(Buffer* outBuffer,
                                double execTime) const {
    auto writer = outBuffer->getWriter();

    // Write http response header
    if (!writer.writeString(headerOk)) {
        return false;
    }
    if (!writer.writeString(emptyLine)) {
        return false;
    }

    // Begin
    if (!writer.writeString(bodyBegin)) {
        return false;
    }

    // Status
    const std::string& statusStr = getQueryStatusString(QueryStatus::OK);
    if (!writer.writeString(statusStr)) {
        return false;
    }

    // Time
    if (!writer.writeString(timeBeginStr)) {
        return false;
    }

    const std::string durationStr = std::to_string(execTime)+"ms";
    if (!writer.writeString(durationStr)) {
        return false;
    }

    if (!writer.writeString(timeEndStr)) {
        return false;
    }

    // Data begin
    if (!writer.writeString(bodyDataBegin)) {
        return false;
    }

    // End
    if (!writer.writeString(bodyEnd)) {
        return false;
    }

    return true;
}

void Interpreter::handleQueryError(QueryStatus status, Buffer* outBuffer) const {
    auto writer = outBuffer->getWriter();
    writer.reset();

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
    strcpy(writer.getBuffer(), bodyDataBegin.c_str());
    writer.setWrittenBytes(bodyDataBegin.size());

    // End
    strcpy(writer.getBuffer(), bodyEnd.c_str());
    writer.setWrittenBytes(bodyEnd.size());
}
