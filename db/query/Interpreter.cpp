#include "Interpreter.h"

#include "string.h"

#include "QueryParser.h"
#include "plan/Planner.h"
#include "plan/PullPlan.h"

#include "Buffer.h"

using namespace db;

Interpreter::Interpreter(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

Interpreter::~Interpreter() {
}

void Interpreter::execQuery(const std::string& query, Buffer* outBuffer) {
    QueryParser parser;

    QueryCommand* cmd = parser.parse(query);
    if (!cmd) {
        return;
    }

    Planner planner(_interpCtxt);
    PullPlan* pullPlan = planner.makePlan(cmd);
    if (!pullPlan) {
        return;
    }

    {
        auto writer = outBuffer->getWriter();

        strcpy(writer.getBuffer(), headerOk.c_str());
        writer.setWrittenBytes(headerOk.size()); 
        
        strcpy(writer.getBuffer(), emptyLine.c_str());
        writer.setWrittenBytes(emptyLine.size());

        strcpy(writer.getBuffer(), body.c_str());
        writer.setWrittenBytes(body.size());
    }

    delete pullPlan;

    return;
}
