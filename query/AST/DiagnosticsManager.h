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

    /// Renders @p msg against the source location registered for @p obj and throws it as
    /// a CompilerException, so a rejection reports the offending span of the query
    [[noreturn]] void throwError(std::string_view msg, const void* obj) const;

private:
    SourceManager* _sourceManager {nullptr};
};

}
