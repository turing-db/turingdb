#pragma once

#include "ServerThread.h"

namespace ui {

class FlaskThread : public ServerThread {
public:
    FlaskThread();
    ~FlaskThread() override;

private:
    void task() override;
    void devTask() override;
};

}
