#pragma once

#include "Utils.h"

namespace net {

struct ServerContext;

class TCPListener {
public:
    explicit TCPListener(ServerContext& context);

    void accept(utils::EpollEvent& ev);

private:
    ServerContext& _ctxt;
};

}
