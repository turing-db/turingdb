#pragma once

#include <string>
#include <string_view>

namespace db {

class SourceManager;

class DiagnosticsManager {
public:
    explicit DiagnosticsManager(SourceManager* sourceManager);
    ~DiagnosticsManager();

    DiagnosticsManager(const DiagnosticsManager&) = delete;
    DiagnosticsManager(DiagnosticsManager&&) = delete;
    DiagnosticsManager& operator=(const DiagnosticsManager&) = delete;
    DiagnosticsManager& operator=(DiagnosticsManager&&) = delete;

    void createErrorString(std::string_view msg, const void* obj, std::string& result) const;

private:
    SourceManager* _sourceManager {nullptr};
};

}
