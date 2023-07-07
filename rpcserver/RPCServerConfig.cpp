#include "RPCServerConfig.h"
#include "FileUtils.h"
#include <iostream>

RPCServerConfig::RPCServerConfig()
{
    const char* home = std::getenv("HOME");
    _databasesPath = (FileUtils::Path {home} / "databases").string();
}

RPCServerConfig::~RPCServerConfig() {
}
