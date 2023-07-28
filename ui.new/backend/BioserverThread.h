#pragma once

#include "ServerThread.h"

namespace ui {

class BioserverThread : public ServerThread {
public:
    BioserverThread();
    ~BioserverThread() override;

private:
    void task() override;
};

}
