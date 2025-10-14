#include "DiagnosticsManager.h"
#include "CypherError.h"
#include "SourceLocation.h"
#include "SourceManager.h"

using namespace db::v2;

DiagnosticsManager::DiagnosticsManager(SourceManager* sourceManager)
    : _sourceManager(sourceManager)
{
}

DiagnosticsManager::~DiagnosticsManager() {
}

std::string DiagnosticsManager::createErrorString(std::string_view msg, const void* obj) const {
    const SourceLocation* location = _sourceManager->getLocation((uintptr_t)obj);
    std::string errorMsg;

    CypherError err {_sourceManager->getQueryString()};
    err.setTitle("Query error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(errorMsg);

    return errorMsg;
}
