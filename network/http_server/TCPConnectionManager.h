#pragma once

#include "Utils.h"

namespace net {

struct ServerContext;
class AbstractThreadContext;

class TCPConnectionManager {
public:
    explicit TCPConnectionManager(ServerContext& ctxt);

    void process(AbstractThreadContext* threadContext,
                 utils::EpollEvent& ev);

private:
    ServerContext& _ctxt;
};

}
