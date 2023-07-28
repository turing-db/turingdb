#pragma once

#include "ServerThread.h"

namespace ui {

class ReactThread : public ServerThread {
public:
    ReactThread();
    ~ReactThread() override;

private:
    void devTask() override;
};

}
