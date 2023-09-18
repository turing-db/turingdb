#include "ServerCommand.h"

namespace ui {

ServerCommand::ServerCommand(const std::string& name)
    : _name(name)
{
}

ServerCommand::~ServerCommand() {
}

void ServerCommand::terminate() {
    _process->terminate();
    _process->wait();
}

bool ServerCommand::isDone() {
    return !_process->running();
}

int ServerCommand::getReturnCode() const {
    return _process->exit_code();
}

}
