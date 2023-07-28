#pragma once

#include "ServerThread.h"

namespace ui {

class TailwindThread : public ServerThread {
public:
    TailwindThread();
    ~TailwindThread() override;

private:
    void devTask() override;
};

}
