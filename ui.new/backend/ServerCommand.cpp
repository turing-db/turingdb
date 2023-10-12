#include "ServerCommand.h"

namespace ui {

ServerCommand::ServerCommand(const std::string& name)
    : _name(name)
{
}

ServerCommand::~ServerCommand() {
}

void ServerCommand::terminate() {
    if (_process) {
        _process->terminate();
        _process->wait();
    }
}

bool ServerCommand::isDone() {
    if (!_process) {
        return true;
    }
    return !_process->running();
}

int ServerCommand::getReturnCode() const {
    if (!_process) {
        return 1;
    }
    return _process->exit_code();
}

}
