#ifndef _UI_SERVER_
#define _UI_SERVER_

#include <filesystem>

namespace ui {

class TuringUIServer {
public:
    using Path = std::filesystem::path;

    TuringUIServer(const Path& outDir);
    ~TuringUIServer();

    void start();

private:
    bool _cleanEnabled {true};
    Path _outDir;

    void cleanSite();
};

}

#endif
