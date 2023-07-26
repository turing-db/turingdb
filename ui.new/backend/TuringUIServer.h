#ifndef _UI_SERVER_
#define _UI_SERVER_

#include "Command.h"

#include <filesystem>

namespace ui {

class TuringUIServer {
public:
    using Path = std::filesystem::path;

    TuringUIServer(const Path& outDir);
    ~TuringUIServer();

    int start();
    void setCleanEnabled(bool enable) { _cleanEnabled = enable; }
    int getReturnCode() const;
    void getLogs(std::string& data) const;

private:
    bool _cleanEnabled {true};
    Path _outDir;
    Command _cmd;

    void cleanSite();
};

}

#endif
