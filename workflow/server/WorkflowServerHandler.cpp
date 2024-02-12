#include "WorkflowServerHandler.h"

#include "Buffer.h"

using namespace workflow;

void WorkflowServerHandler::process(Buffer* outBuffer,
                                    const std::string& uri,
                                    StringSpan payload) {
    auto writer = outBuffer->getWriter();
    writer.writeString("HTTP/1.1 200 OK\r\n");
    writer.writeString("\r\n");
    writer.writeString("{}\n");
}
