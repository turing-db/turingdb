#pragma once

#include <string>
#include <string_view>

namespace db::v2 {

class SourceManager;

class DiagnosticsManager {
public:
    explicit DiagnosticsManager(SourceManager* sourceManager);
    ~DiagnosticsManager();

    DiagnosticsManager(const DiagnosticsManager&) = delete;
    DiagnosticsManager(DiagnosticsManager&&) = delete;
    DiagnosticsManager& operator=(const DiagnosticsManager&) = delete;
    DiagnosticsManager& operator=(DiagnosticsManager&&) = delete;

    std::string createErrorString(std::string_view msg, const void* obj) const;

private:
    SourceManager* _sourceManager {nullptr};
};

}
