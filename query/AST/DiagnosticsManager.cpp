#include "DiagnosticsManager.h"

#include "CompilerException.h"
#include "CypherError.h"
#include "SourceLocation.h"
#include "SourceManager.h"

using namespace db;

DiagnosticsManager::DiagnosticsManager(SourceManager* sourceManager)
    : _sourceManager(sourceManager)
{
}

DiagnosticsManager::~DiagnosticsManager() {
}

void DiagnosticsManager::createErrorString(std::string_view msg,
                                           const void* obj,
                                           std::string& result) const {
    const SourceLocation* location = _sourceManager->getLocation((uintptr_t)obj);

    CypherError err(_sourceManager->getQueryString());
    err.setTitle("Query error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(result);
}

void DiagnosticsManager::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    createErrorString(msg, obj, errorStr);

    throw CompilerException(std::move(errorStr));
}
